"""
Simple script to create Pavlovian behavior metadata with default settings.

User should use this script to create a new session metadata file,
modifying specific fields as needed.
"""

from pathlib import Path
from typing import List
import logging
import sys

from aind_metadata_mapper.pavlovian_behavior.session import ETL
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import VolumeUnit


def create_metadata(
    subject_id: str,
    data_directory: Path,
    output_directory: Path,
    output_filename: str = "session_pavlovian.json",
    experimenter_full_name: List[str] = [
        "test_experimenter_1",
        "test_experimenter_2",
    ],
    rig_id: str = "428_9_0_20240617",
    iacuc_protocol: str = "2115",
    mouse_platform_name: str = "mouse_tube_foraging",
    active_mouse_platform: bool = False,
    session_type: str = "Pavlovian_Conditioning",
    task_name: str = "Pavlovian_Conditioning",
    notes: str = ("Example configuration for Pavlovian Conditioning behavior"),
    reward_units_per_trial: float = 2.0,
    reward_consumed_unit: VolumeUnit = VolumeUnit.UL,
) -> bool:
    """Create Pavlovian behavior metadata with default settings.

    Args:
        subject_id: Subject identifier
        data_directory: Path to behavior data directory
        output_directory: Directory where metadata will be saved
        output_filename: Name of the output JSON file
        experimenter_full_name: List of experimenter names
        rig_id: Identifier for the experimental rig
        iacuc_protocol: Protocol identifier
        mouse_platform_name: Name of the mouse platform
        active_mouse_platform: Whether platform is active
        session_type: Type of experimental session
        task_name: Name of the experimental task
        notes: Additional notes about the session
        reward_units_per_trial: Number of reward units per successful trial
        reward_consumed_unit: Unit of reward consumed

    Returns:
        bool: True if metadata was successfully created and verified
    """
    # Create settings with defaults
    settings = {
        "subject_id": subject_id,
        "experimenter_full_name": experimenter_full_name,
        "data_directory": str(data_directory),
        "output_directory": str(output_directory),
        "output_filename": output_filename,
        "rig_id": rig_id,
        "iacuc_protocol": iacuc_protocol,
        "mouse_platform_name": mouse_platform_name,
        "active_mouse_platform": active_mouse_platform,
        "session_type": session_type,
        "task_name": task_name,
        "notes": notes,
        "reward_units_per_trial": reward_units_per_trial,
        "reward_consumed_unit": reward_consumed_unit,
        "data_streams": [
            {
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
        ],
        "stimulus_epochs": [],
    }

    # Create JobSettings instance and run ETL
    job_settings = JobSettings(**settings)
    etl = ETL(job_settings)
    response = etl.run_job()

    if response.status_code != 200:
        logging.error(f"ETL job failed: {response.message}")
        return False

    return True  # If we get here, ETL job succeeded and file was verified


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Create Pavlovian behavior metadata with default settings"
    )
    parser.add_argument(
        "--subject-id", type=str, required=True, help="Subject identifier"
    )
    parser.add_argument(
        "--data-directory",
        type=Path,
        required=True,
        help="Path to behavior data directory",
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path.cwd(),
        help=(
            "Directory where metadata will be saved "
            "(default: current directory)"
        ),
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_pavlovian.json",
        help="Name of the output JSON file (default: session_pavlovian.json)",
    )

    args = parser.parse_args()

    success = create_metadata(
        subject_id=args.subject_id,
        data_directory=args.data_directory,
        output_directory=args.output_directory,
        output_filename=args.output_filename,
    )

    output_path = args.output_directory / args.output_filename
    if success:
        print(f"Metadata successfully saved and verified at: {output_path}")
    else:
        print(f"Failed to create or verify metadata at: {output_path}")
        sys.exit(1)
