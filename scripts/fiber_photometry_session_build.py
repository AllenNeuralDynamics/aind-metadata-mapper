#!/usr/bin/env python3
"""Script for generating Fiber Photometry session metadata."""

import argparse
import sys
from pathlib import Path

from aind_metadata_mapper.fiber_photometry.session import ETL
from aind_metadata_mapper.fiber_photometry.models import JobSettings
from aind_metadata_mapper.utils.cli import (
    load_config,
    resolve_parameters,
    create_common_parser,
    run_etl,
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

    # Run the ETL process
    run_etl(args, required_params, ETL, JobSettings)


if __name__ == "__main__":
    main()
