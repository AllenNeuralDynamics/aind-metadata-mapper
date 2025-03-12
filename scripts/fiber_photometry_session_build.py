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
from aind_metadata_mapper.utils.cli import (
    load_config,
    resolve_parameters,
    create_common_parser,
)


def parse_args():
    """Parse command line arguments."""
    # Create parser with common arguments
    parser = create_common_parser("Generate Fiber Photometry session metadata")

    # Add fiber photometry-specific arguments
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_fiber_photometry.json",
        help="Name of the output file (defaults to session_fiber_photometry.json)",
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
    if response.message:
        print(f"Response message: {response.message}")

    # Define the output path
    output_path = os.path.join(
        params["output_directory"], params["output_filename"]
    )

    if response.data:
        # If we have data in the response, write it to the output file
        with open(output_path, "w") as f:
            f.write(response.data)
        print(f"Session metadata saved to: {output_path}")
    else:
        # If no data in response, the file was likely written with a different name
        # Let's check if a session.json file was created
        session_file = os.path.join(params["output_directory"], "session.json")
        if os.path.exists(session_file):
            # Copy the file to the desired output path
            with open(session_file, "r") as src:
                with open(output_path, "w") as dst:
                    dst.write(src.read())
            print(
                f"Session metadata copied from {session_file} to {output_path}"
            )
        else:
            print(
                f"Warning: No output file found at {output_path} or {session_file}"
            )


if __name__ == "__main__":
    main()
