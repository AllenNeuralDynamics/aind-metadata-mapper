#!/usr/bin/env python3
"""
Create a unified session metadata file by generating and merging
Pavlovian behavior and fiber photometry metadata.

This script serves as a single entry point for:
1. Generating Pavlovian behavior session metadata
2. Generating fiber photometry session metadata
3. Merging the two session files into a unified metadata file

Example Usage:
    To create a unified session metadata file from the command line:

    ```bash
    python scripts/example_create_fiber_and_pav_sessions.py \
        --subject-id "000000" \
        --behavior-dir data/sample_fiber_data/behavior \
        --fiber-dir data/sample_fiber_data/fib \
        --output-dir data/sample_fiber_data \
        --experimenters "Test User 1" "Test User 2" \
        --behavior-output "session_pavlovian.json" \
        --fiber-output "session_fib.json" \
        --merged-output "session.json"
    ```

    This will:
    1. Generate Pavlovian behavior metadata in 'pav_behavior.json'
    2. Generate fiber photometry metadata in 'fiber_phot.json'
    3. Merge both files into a unified 'session_combined.json'

    All optional parameters (rig_id, iacuc, notes, etc.)
    will use default values unless specified.
    See --help for full list of options.
"""

import sys
from pathlib import Path
from typing import List
import logging

from aind_metadata_mapper.pavlovian_behavior.session import ETL as BehaviorETL
from aind_metadata_mapper.pavlovian_behavior.models import (
    JobSettings as BehaviorJobSettings,
)
from aind_metadata_mapper.fip.session import FIBEtl as FiberETL
from aind_metadata_mapper.fip.models import JobSettings as FiberJobSettings
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import VolumeUnit
from aind_metadata_mapper.utils.merge_sessions import merge_sessions


def create_unified_session(
    subject_id: str,
    behavior_data_dir: Path,
    fiber_data_dir: Path,
    output_dir: Path,
    experimenter_names: List[str],
    behavior_output: str = "session_pavlovian.json",
    fiber_output: str = "session_fib.json",
    merged_output: str = "session.json",
    rig_id: str = "428_9_0_20240617",
    iacuc_protocol: str = "2115",
    session_notes: str = "",
    reward_volume_per_trial: float = 2.0,
    reward_volume_unit: str = "microliter",
) -> bool:
    """Create a unified session metadata file from behavior and fiber data.

    Args:
        subject_id: Subject identifier
        behavior_data_dir: Directory containing Pavlovian behavior data
        fiber_data_dir: Directory containing fiber photometry data
        output_dir: Directory where metadata files will be saved
        experimenter_names: List of experimenter full names
        behavior_output: Filename for behavior session metadata
        fiber_output: Filename for fiber photometry session metadata
        merged_output: Filename for merged session metadata
        rig_id: Identifier for the experimental rig
        iacuc_protocol: Protocol identifier
        session_notes: Additional notes about the session
        reward_volume_per_trial: Volume of reward delivered per successful
            trial
        reward_volume_unit: Unit of reward volume (e.g., 'microliter',
            'milliliter')

    Returns:
        bool: True if all operations completed successfully
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create behavior settings
    behavior_settings = {
        "subject_id": subject_id,
        "experimenter_full_name": experimenter_names,
        "data_directory": str(behavior_data_dir),
        "output_directory": str(output_dir),
        "output_filename": behavior_output,
        "rig_id": rig_id,
        "iacuc_protocol": iacuc_protocol,
        "mouse_platform_name": "mouse_tube_foraging",
        "active_mouse_platform": False,
        "session_type": "Pavlovian_Conditioning",
        "task_name": "Pavlovian_Conditioning",
        "notes": session_notes,
        "reward_units_per_trial": reward_volume_per_trial,
        "reward_consumed_unit": VolumeUnit.UL,
        "data_streams": [get_behavior_data_stream()],
        "stimulus_epochs": [],
    }

    # Create fiber settings
    fiber_settings = {
        "subject_id": subject_id,
        "experimenter_full_name": experimenter_names,
        "data_directory": str(fiber_data_dir),
        "output_directory": str(output_dir),
        "output_filename": fiber_output,
        "rig_id": rig_id,
        "task_version": "1.0.0",
        "iacuc_protocol": iacuc_protocol,
        "mouse_platform_name": "mouse_tube_foraging",
        "active_mouse_platform": False,
        "session_type": "Foraging_Photometry",
        "task_name": "Fiber Photometry",
        "notes": session_notes,
        "data_streams": [get_fiber_data_stream()],
    }

    # Run behavior ETL
    logging.info("Generating Pavlovian behavior metadata...")
    behavior_job = BehaviorJobSettings(**behavior_settings)
    behavior_etl = BehaviorETL(behavior_job)
    behavior_response = behavior_etl.run_job()

    if behavior_response.status_code != 200:
        logging.error(
            f"Failed to generate behavior metadata: {behavior_response.message}"  # noqa: E501
        )
        return False

    # Run fiber ETL
    logging.info("Generating fiber photometry metadata...")
    fiber_job = FiberJobSettings(**fiber_settings)
    fiber_etl = FiberETL(fiber_job)
    fiber_response = fiber_etl.run_job()

    if fiber_response.status_code != 200:
        logging.error(
            f"Failed to generate fiber metadata: {fiber_response.message}"
        )
        return False

    logging.info("Merging session metadata files...")
    try:
        merge_sessions(
            session_file1=output_dir / behavior_output,
            session_file2=output_dir / fiber_output,
            output_file=output_dir / merged_output,
        )
    except Exception as e:
        logging.error(f"Failed to merge session files: {e}")
        return False

    logging.info(
        "Successfully created unified session metadata at: "
        f"{output_dir / merged_output}"
    )
    return True


def get_fiber_data_stream() -> dict:
    """Get default fiber photometry data stream configuration.

    Returns:
        dict: Default fiber photometry data stream configuration
    """
    return {
        "stream_start_time": None,
        "stream_end_time": None,
        "stream_modalities": ["FIB"],
        "camera_names": [],
        "daq_names": [""],
        "detectors": [
            {
                "exposure_time": "5230.42765",
                "exposure_time_unit": "millisecond",
                "name": "Green CMOS",
                "trigger_type": "Internal",
            },
            {
                "exposure_time": "5230.42765",
                "exposure_time_unit": "millisecond",
                "name": "Red CMOS",
                "trigger_type": "Internal",
            },
        ],
        "ephys_modules": [],
        "fiber_connections": [
            {
                "fiber_name": "Fiber 0",
                "output_power_unit": "microwatt",
                "patch_cord_name": "Patch Cord 0",
                "patch_cord_output_power": "20",
            },
            {
                "fiber_name": "Fiber 1",
                "output_power_unit": "microwatt",
                "patch_cord_name": "Patch Cord 1",
                "patch_cord_output_power": "20",
            },
            {
                "fiber_name": "Fiber 2",
                "output_power_unit": "microwatt",
                "patch_cord_name": "Patch Cord 2",
                "patch_cord_output_power": "20",
            },
            {
                "fiber_name": "Fiber 3",
                "output_power_unit": "microwatt",
                "patch_cord_name": "Patch Cord 3",
                "patch_cord_output_power": "20",
            },
        ],
        "fiber_modules": [],
        "light_sources": [
            {
                "device_type": "Light emitting diode",
                "excitation_power": None,
                "excitation_power_unit": "milliwatt",
                "name": "470nm LED",
            },
            {
                "device_type": "Light emitting diode",
                "excitation_power": None,
                "excitation_power_unit": "milliwatt",
                "name": "415nm LED",
            },
            {
                "device_type": "Light emitting diode",
                "excitation_power": None,
                "excitation_power_unit": "milliwatt",
                "name": "565nm LED",
            },
        ],
        "manipulator_modules": [],
        "mri_scans": [],
        "notes": "Fib modality: fib mode: Normal",
        "ophys_fovs": [],
        "slap_fovs": [],
        "software": [
            {
                "name": "Bonsai",
                "parameters": {},
                "url": "",
                "version": "",
            }
        ],
        "stack_parameters": None,
        "stick_microscopes": [],
    }


def get_behavior_data_stream() -> dict:
    """Get default Pavlovian behavior data stream configuration.

    Returns:
        dict: Default Pavlovian behavior data stream configuration
    """
    return {
        "stream_start_time": None,
        "stream_end_time": None,
        "stream_modalities": [Modality.BEHAVIOR],
        "camera_names": [],
        "daq_names": [""],
        "light_sources": [
            {
                "device_type": "Light emitting diode",
                "excitation_power": None,
                "excitation_power_unit": "milliwatt",
                "name": "IR LED",
            }
        ],
        "notes": "Behavioral tracking with IR LED",
        "software": [
            {
                "name": "Bonsai",
                "parameters": {},
                "url": "",
                "version": "",
            }
        ],
    }


def main():
    """Command line interface for creating unified session metadata."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Create unified session metadata from behavior and fiber data"  # noqa: E501
    )
    parser.add_argument(
        "--subject-id", type=str, required=True, help="Subject identifier"
    )
    parser.add_argument(
        "--behavior-dir",
        type=Path,
        required=True,
        help="Directory containing Pavlovian behavior data",
    )
    parser.add_argument(
        "--fiber-dir",
        type=Path,
        required=True,
        help="Directory containing fiber photometry data",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        help="Directory where metadata files will be saved "
        "(default: current directory)",
    )
    parser.add_argument(
        "--experimenters",
        type=str,
        nargs="+",
        required=True,
        help="List of experimenter full names",
    )
    parser.add_argument(
        "--rig-id",
        type=str,
        default="428_9_0_20240617",
        help="Identifier for the experimental rig",
    )
    parser.add_argument(
        "--iacuc",
        type=str,
        default="2115",
        help="IACUC protocol identifier",
    )
    parser.add_argument(
        "--notes",
        type=str,
        default="",
        help="Additional notes about the session",
    )
    parser.add_argument(
        "--reward-volume",
        type=float,
        default=2.0,
        help="Volume of reward delivered per successful trial",
    )
    parser.add_argument(
        "--reward-unit",
        type=str,
        choices=["microliter", "milliliter"],
        default="microliter",
        help="Unit of reward volume",
    )
    parser.add_argument(
        "--behavior-output",
        type=str,
        default="session_pavlovian.json",
        help=(
            "Filename for behavior session metadata "
            "(default: session_pavlovian.json)"
        ),
    )
    parser.add_argument(
        "--fiber-output",
        type=str,
        default="session_fib.json",
        help=(
            "Filename for fiber photometry session metadata "
            "(default: session_fib.json)"
        ),
    )
    parser.add_argument(
        "--merged-output",
        type=str,
        default="session.json",
        help="Filename for merged session metadata (default: session.json)",
    )

    args = parser.parse_args()

    success = create_unified_session(
        subject_id=args.subject_id,
        behavior_data_dir=args.behavior_dir,
        fiber_data_dir=args.fiber_dir,
        output_dir=args.output_dir,
        experimenter_names=args.experimenters,
        behavior_output=args.behavior_output,
        fiber_output=args.fiber_output,
        merged_output=args.merged_output,
        rig_id=args.rig_id,
        iacuc_protocol=args.iacuc,
        session_notes=args.notes,
        reward_volume_per_trial=args.reward_volume,
        reward_volume_unit=args.reward_unit,
    )

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="\n%(asctime)s - %(message)s\n",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
