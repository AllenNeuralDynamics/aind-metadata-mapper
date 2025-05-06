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
        --data-dir data/sample_fiber_data \
        --output-dir data/sample_fiber_data \
        --experimenters "Test User 1" "Test User 2" \
        --session-type "Pavlovian_Conditioning + FIB" \
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
import logging

from aind_metadata_mapper.pavlovian_behavior.example_create_session import (
    create_metadata as create_pavlovian_metadata,
)
from aind_metadata_mapper.fip.example_create_session import (
    create_metadata as create_fip_metadata,
)
from aind_metadata_mapper.utils.merge_sessions import merge_sessions
from aind_data_schema.core.session import Session


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Create unified session metadata from behavior and fiber data"
    )
    parser.add_argument(
        "--subject-id", type=str, required=True, help="Subject identifier"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Root directory containing 'behavior' and 'fib' subdirectories",
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
        default=None,
        help="Identifier for the experimental rig",
    )
    parser.add_argument(
        "--iacuc",
        type=str,
        default=None,
        help="IACUC protocol identifier",
    )
    parser.add_argument(
        "--notes",
        type=str,
        default=None,
        help="Additional notes about the session",
    )
    parser.add_argument(
        "--reward-volume",
        type=float,
        default=None,
        help="Volume of reward delivered per successful trial",
    )
    parser.add_argument(
        "--reward-unit",
        type=str,
        choices=["microliter", "milliliter"],
        default=None,
        help="Unit of reward volume",
    )
    parser.add_argument(
        "--session-type",
        type=str,
        default=None,
        help="Session type to use for both behavior and fiber metadata (overrides individual defaults if specified)",
    )
    parser.add_argument(
        "--behavior-output",
        type=str,
        default="session_pavlovian.json",
        help="Filename for behavior session metadata (default: session_pavlovian.json)",
    )
    parser.add_argument(
        "--fiber-output",
        type=str,
        default="session_fib.json",
        help="Filename for fiber photometry session metadata (default: session_fib.json)",
    )
    parser.add_argument(
        "--merged-output",
        type=str,
        default="session.json",
        help="Filename for merged session metadata (default: session.json)",
    )

    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build a dict of all possible arguments
    pav_cli_kwargs = {
        "subject_id": args.subject_id,
        "data_directory": args.data_dir,
        "output_directory": output_dir,
        "output_filename": args.behavior_output,
        "experimenter_full_name": args.experimenters,
        "rig_id": args.rig_id,
        "iacuc_protocol": args.iacuc,
        "notes": args.notes,
        "reward_units_per_trial": args.reward_volume,
        "reward_consumed_unit": args.reward_unit,
        "session_type": args.session_type,
    }

    print(f"pav_cli_kwargs: {pav_cli_kwargs}")

    # Only include those that are not None
    pav_kwargs = {k: v for k, v in pav_cli_kwargs.items() if v is not None}

    # Run Pavlovian behavior ETL
    logging.info("Generating Pavlovian behavior metadata...")
    pav_success = create_pavlovian_metadata(**pav_kwargs)

    if not pav_success:
        logging.error("Failed to generate Pavlovian behavior metadata.")
        sys.exit(1)

    # Repeat for fiber
    fip_cli_kwargs = {
        "subject_id": args.subject_id,
        "data_directory": args.data_dir,
        "output_directory": output_dir,
        "output_filename": args.fiber_output,
        "experimenter_full_name": args.experimenters,
        "rig_id": args.rig_id,
        "iacuc_protocol": args.iacuc,
        "notes": args.notes,
        "session_type": args.session_type,
    }
    fip_kwargs = {k: v for k, v in fip_cli_kwargs.items() if v is not None}

    print(f"fip_kwargs: {fip_kwargs}")

    # Run fiber photometry ETL
    logging.info("Generating fiber photometry metadata...")
    fib_success = create_fip_metadata(**fip_kwargs)

    if not fib_success:
        logging.error("Failed to generate fiber photometry metadata.")
        sys.exit(1)

    # Merge the two session files
    logging.info("Merging session metadata files...")
    try:
        merged = merge_sessions(
            session_file1=output_dir / args.behavior_output,
            session_file2=output_dir / args.fiber_output,
            output_file=output_dir / args.merged_output,
        )
    except Exception as e:
        logging.error(f"Failed to merge session files: {e}")
        sys.exit(1)

    session_model = Session(**merged)
    with open(output_dir / args.merged_output, "w") as f:
        f.write(session_model.model_dump_json(indent=2))

    logging.info(
        f"Successfully created unified session metadata at: {output_dir / args.merged_output}"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="\n%(asctime)s - %(message)s\n",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
