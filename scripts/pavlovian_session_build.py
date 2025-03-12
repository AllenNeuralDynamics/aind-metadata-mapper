#!/usr/bin/env python3
"""
Example script for generating Pavlovian Behavior session metadata.

This script demonstrates how to use the ETL class to generate
session metadata by extracting data from behavior files.
"""

import argparse
import sys
import os
import json
from pathlib import Path
from typing import Dict, Any, Optional

from aind_metadata_mapper.pavlovian_behavior.session import ETL
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings
from aind_metadata_mapper.utils.cli import (
    load_config,
    resolve_parameters,
    create_common_parser,
    run_etl,
)


def parse_args():
    """Parse command line arguments."""
    # Create parser with common arguments
    parser = create_common_parser(
        "Generate Pavlovian Behavior session metadata"
    )

    # Add Pavlovian-specific arguments
    parser.add_argument(
        "--task-name",
        type=str,
        help="Name of the task",
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_pavlovian_behavior.json",
        help="Name of the output file (defaults to session_pavlovian_behavior.json)",
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
        "task_name",
    ]

    # Run the ETL process
    run_etl(args, required_params, ETL, JobSettings)


if __name__ == "__main__":
    main()
