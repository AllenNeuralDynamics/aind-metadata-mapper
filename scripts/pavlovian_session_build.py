#!/usr/bin/env python3
"""
Example script for generating Pavlovian Behavior session metadata.

This script demonstrates how to use the BehaviorEtl class to generate
session metadata by extracting data from behavior files.
"""

import argparse
import sys
import os
from pathlib import Path

from aind_metadata_mapper.pavlovian_behavior.session import BehaviorEtl
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate Pavlovian Behavior session metadata"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Path to directory containing behavior data files",
    )
    parser.add_argument(
        "--subject-id",
        type=str,
        required=True,
        help="Subject identifier",
    )
    parser.add_argument(
        "--experimenter",
        type=str,
        required=True,
        help="Experimenter full name",
    )
    parser.add_argument(
        "--rig-id",
        type=str,
        required=True,
        help="Identifier for the experimental rig",
    )
    parser.add_argument(
        "--task-version",
        type=str,
        default="1.0.0",
        help="Version of the Pavlovian task",
    )
    parser.add_argument(
        "--iacuc-protocol",
        type=str,
        default="2115",
        help="IACUC protocol identifier (defaults to 2115)",
    )
    parser.add_argument(
        "--mouse-platform-name",
        type=str,
        default="mouse_tube_foraging",
        help="Name of the mouse platform used (defaults to mouse_tube_foraging)",
    )
    parser.add_argument(
        "--active-mouse-platform",
        action="store_true",
        default=True,
        help="Whether the mouse platform was active (defaults to True)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write session.json file (defaults to data directory)",
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_pavlovian_behavior.json",
        help="Name of the output file (defaults to session_pavlovian_behavior.json)",
    )

    return parser.parse_args()


def main():
    """Run the Pavlovian Behavior ETL process."""
    args = parse_args()

    # Create settings with path to data directory
    settings = JobSettings(
        experimenter_full_name=[args.experimenter],
        subject_id=args.subject_id,
        rig_id=args.rig_id,
        task_version=args.task_version,
        data_directory=args.data_dir,
        iacuc_protocol=args.iacuc_protocol,
        output_directory=args.output_dir or args.data_dir,
        output_filename=args.output_filename,
        mouse_platform_name=args.mouse_platform_name,
        active_mouse_platform=args.active_mouse_platform,
    )

    # Generate session metadata - data will be extracted from files
    print(f"Extracting data from {args.data_dir}")
    etl = BehaviorEtl(settings)
    response = etl.run_job()

    # Output path for reference
    output_dir = args.output_dir or args.data_dir
    output_path = os.path.join(output_dir, args.output_filename)

    # Check if the file was written successfully
    if os.path.exists(output_path):
        print(f"Session metadata saved to: {output_path}")
    elif response.data is not None:
        # If we have data but no file, write it
        with open(output_path, "w") as f:
            f.write(response.data)
        print(f"Session metadata saved to: {output_path}")
    else:
        print(f"Warning: Failed to generate session metadata")

    return 0


if __name__ == "__main__":
    sys.exit(main())
