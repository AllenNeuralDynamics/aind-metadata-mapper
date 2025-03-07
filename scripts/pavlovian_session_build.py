#!/usr/bin/env python3
"""
Example script for generating Pavlovian Behavior session metadata.

This script demonstrates how to use the BehaviorEtl class to generate
session metadata by extracting data from behavior files.
"""

import argparse
import sys
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
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write session.json file (defaults to data directory)",
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
        required=True,
        help="IACUC protocol identifier",
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
    )

    # Generate session metadata - data will be extracted from files
    print(f"Extracting data from {args.data_dir}")
    etl = BehaviorEtl(settings)
    response = etl.run_job()

    # Print the path to the generated session.json file
    if response.output_file:
        print(f"Session metadata saved to: {response.output_file}")
    else:
        print("Session metadata generated but not saved to file.")
        print(f"Response data: {response.data}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
