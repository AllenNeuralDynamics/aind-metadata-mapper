#!/usr/bin/env python3
"""Script for generating Fiber Photometry session metadata."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Any

from aind_metadata_mapper.fiber_photometry.session import FIBEtl
from aind_metadata_mapper.fiber_photometry.models import JobSettings
from aind_metadata_mapper.utils.cli import load_config, resolve_parameters


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate Fiber Photometry session metadata"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Path to directory containing fiber photometry data files",
    )
    parser.add_argument(
        "--session-params",
        type=str,
        help="Path to session parameters JSON file",
    )
    parser.add_argument(
        "--data-streams",
        type=str,
        help="Path to data streams JSON file",
    )
    parser.add_argument(
        "--subject-id",
        type=str,
        help="Subject identifier",
    )
    parser.add_argument(
        "--experimenter-full-name",
        type=str,
        help="Experimenter full name",
    )
    parser.add_argument(
        "--rig-id",
        type=str,
        help="Identifier for the experimental rig",
    )
    parser.add_argument(
        "--task-version",
        type=str,
        help="Version of the task",
    )
    parser.add_argument(
        "--iacuc-protocol",
        type=str,
        help="IACUC protocol identifier",
    )
    parser.add_argument(
        "--mouse-platform-name",
        type=str,
        help="Name of the mouse platform used",
    )
    parser.add_argument(
        "--active-mouse-platform",
        type=str,
        choices=["true", "false"],
        help="Whether the mouse platform was active (specify 'true' or 'false')",
    )
    parser.add_argument(
        "--session-type",
        type=str,
        help="Type of session",
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_fiber_photometry.json",
        help="Name of the output file (defaults to session_fiber_photometry.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write output file to (defaults to data directory)",
    )

    return parser.parse_args()


def main():
    """Main entry point for the script."""
    args = parse_args()

    # Define required parameters
    required_params = [
        "subject_id",
        "experimenter_full_name",
        "rig_id",
        "task_version",
        "iacuc_protocol",
        "mouse_platform_name",
        "session_type",
    ]

    # Resolve parameters from command line args and config files
    params = resolve_parameters(args, required_params)

    # Set data_directory
    params["data_directory"] = args.data_dir
    print(f"Extracting data from {params['data_directory']}")

    # Set output directory and filename
    if args.output_dir:
        params["output_directory"] = args.output_dir
    else:
        params["output_directory"] = args.data_dir

    params["output_filename"] = args.output_filename

    # Create JobSettings object
    job_settings = JobSettings(**params)

    # Generate session metadata
    etl = FIBEtl(job_settings)
    response = etl.run_job()

    # Handle the response
    if response.output_file:
        print(f"Session metadata saved to: {response.output_file}")
    elif response.data:
        # If no output file was specified, write to the default location
        output_path = os.path.join(
            params["output_directory"], params["output_filename"]
        )
        with open(output_path, "w") as f:
            f.write(response.data)
        print(f"Session metadata saved to: {output_path}")
    else:
        print("No data returned from ETL process.")


if __name__ == "__main__":
    main()
