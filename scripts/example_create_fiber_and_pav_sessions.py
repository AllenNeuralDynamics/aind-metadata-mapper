#!/usr/bin/env python3
"""
Create a unified session metadata file by generating and merging
Pavlovian behavior and fiber photometry metadata.

This script serves as a single entry point for:
1. Generating Pavlovian behavior session metadata
2. Generating fiber photometry session metadata
3. Merging the two session files into a unified metadata file
"""

import sys
from pathlib import Path
from typing import List
import logging

from aind_metadata_mapper.pavlovian_behavior.example_create_session import (
    create_metadata as create_behavior_metadata,
)
from aind_metadata_mapper.fib.example_create_session import (
    create_metadata as create_fiber_metadata,
)
from aind_metadata_mapper.utils.merge_sessions import merge_sessions


def create_unified_session(
    subject_id: str,
    behavior_data_dir: Path,
    fiber_data_dir: Path,
    output_dir: Path,
    experimenter_names: List[str],
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
        rig_id: Identifier for the experimental rig
        iacuc_protocol: Protocol identifier
        session_notes: Additional notes about the session
        reward_volume_per_trial: Volume of reward delivered per successful trial
        reward_volume_unit: Unit of reward volume (e.g., 'microliter', 'milliliter')

    Returns:
        bool: True if all operations completed successfully
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define output filenames
    behavior_file = "session_pavlovian.json"
    fiber_file = "session_fib.json"
    merged_file = "session_unified.json"

    logging.info("Generating Pavlovian behavior metadata...")
    behavior_success = create_behavior_metadata(
        subject_id=subject_id,
        data_directory=behavior_data_dir,
        output_directory=output_dir,
        output_filename=behavior_file,
        experimenter_full_name=experimenter_names,
        rig_id=rig_id,
        iacuc_protocol=iacuc_protocol,
        notes=session_notes,
        reward_units_per_trial=reward_volume_per_trial,
    )

    if not behavior_success:
        logging.error("Failed to generate behavior metadata")
        return False

    logging.info("Generating fiber photometry metadata...")
    fiber_success = create_fiber_metadata(
        subject_id=subject_id,
        data_directory=fiber_data_dir,
        output_directory=output_dir,
        output_filename=fiber_file,
        experimenter_full_name=experimenter_names,
        rig_id=rig_id,
        iacuc_protocol=iacuc_protocol,
        notes=session_notes,
    )

    if not fiber_success:
        logging.error("Failed to generate fiber metadata")
        return False

    logging.info("Merging session metadata files...")
    try:
        merge_sessions(
            session_file1=output_dir / behavior_file,
            session_file2=output_dir / fiber_file,
            output_file=output_dir / merged_file,
        )
    except Exception as e:
        logging.error(f"Failed to merge session files: {e}")
        return False

    logging.info(
        f"Successfully created unified session metadata at: {output_dir / merged_file}"
    )
    return True


def main():
    """Command line interface for creating unified session metadata."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Create unified session metadata from behavior and fiber data"
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
        help="Directory where metadata files will be saved (default: current directory)",
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

    args = parser.parse_args()

    success = create_unified_session(
        subject_id=args.subject_id,
        behavior_data_dir=args.behavior_dir,
        fiber_data_dir=args.fiber_dir,
        output_dir=args.output_dir,
        experimenter_names=args.experimenters,
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
