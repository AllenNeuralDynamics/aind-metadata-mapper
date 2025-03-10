#!/usr/bin/env python3
"""
Example script for generating Pavlovian Behavior session metadata.

This script demonstrates how to use the BehaviorEtl class to generate
session metadata by extracting data from behavior files.
"""

import argparse
import sys
import os
import json
from pathlib import Path
from typing import Dict, Any, Optional

from aind_metadata_mapper.pavlovian_behavior.session import BehaviorEtl
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a JSON file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Dictionary containing the configuration
    """
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"Warning: Configuration file {config_path} not found.")
        return {}
    except json.JSONDecodeError:
        print(f"Warning: Configuration file {config_path} is not valid JSON.")
        return {}


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
        help="Subject identifier",
    )
    parser.add_argument(
        "--experimenter",
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
        help="Version of the Pavlovian task",
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
        action="store_true",
        help="Whether the mouse platform was active",
    )
    parser.add_argument(
        "--session-type",
        type=str,
        help="Type of session",
    )
    parser.add_argument(
        "--task-name",
        type=str,
        help="Name of the task",
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
    parser.add_argument(
        "--session-params",
        type=str,
        help="Path to session parameters configuration file",
    )
    parser.add_argument(
        "--data-streams",
        type=str,
        help="Path to data streams configuration file",
    )

    return parser.parse_args()


def resolve_parameters(args):
    """Resolve parameters from command line args and config files.

    Args:
        args: Command line arguments

    Returns:
        Dictionary of resolved parameters
    """
    # Start with empty parameters
    params = {}

    # Load session parameters config if specified
    if args.session_params:
        session_config = load_config(args.session_params)
        params.update(session_config)

    # Load data streams config if specified
    data_streams = []
    if args.data_streams:
        data_streams = load_config(args.data_streams)
        # The data_streams file is now a direct list of stream objects

        # Handle the case where the file is empty or not a list
        if not isinstance(data_streams, list):
            print(
                f"Warning: Data streams file {args.data_streams} does not contain a valid list. Using empty list."
            )
            data_streams = []

        # Process data streams to ensure required fields are set
        for stream in data_streams:
            # If stream_start_time or stream_end_time are null, they will be set later
            # based on the session start and end times
            pass

    # Override with command line arguments (only if not None)
    cmd_args = vars(args)
    for key, value in cmd_args.items():
        if value is not None and key not in ["session_params", "data_streams"]:
            # Convert kebab-case to snake_case for parameter names
            param_key = key.replace("-", "_")

            # Check if this parameter is also in the config and warn if different
            if param_key in params and params[param_key] != value:
                print(
                    f"Warning: Parameter '{param_key}' specified both in config ({params[param_key]}) "
                    f"and command line ({value}). Using command line value."
                )

            params[param_key] = value

    # Add data streams to parameters
    if data_streams:
        params["data_streams"] = data_streams

    # Check for required parameters
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

    missing_params = [
        param for param in required_params if param not in params
    ]

    # Special handling for experimenter which is passed as "experimenter" but needed as "experimenter_full_name"
    if "experimenter" in params and "experimenter_full_name" not in params:
        params["experimenter_full_name"] = params["experimenter"]
        missing_params = [
            p for p in missing_params if p != "experimenter_full_name"
        ]

    if missing_params:
        print(
            f"Error: Missing required parameters: {', '.join(missing_params)}"
        )
        sys.exit(1)

    return params


def main():
    """Run the Pavlovian Behavior ETL process."""
    args = parse_args()

    # Resolve parameters from command line and config files
    try:
        params = resolve_parameters(args)
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    # Ensure data_directory is set
    params["data_directory"] = args.data_dir

    # Set output directory and filename
    params["output_directory"] = args.output_dir or args.data_dir
    params["output_filename"] = args.output_filename

    # Create settings with resolved parameters
    settings = JobSettings(**params)

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
