"""Command-line interface utilities for AIND metadata mapper."""

import json
import sys
import os
import argparse
from typing import Dict, Any, List, Type, Callable


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


def create_common_parser(description: str) -> argparse.ArgumentParser:
    """Create an argument parser with common arguments for ETL scripts.

    Args:
        description: Description of the script

    Returns:
        ArgumentParser with common arguments
    """
    parser = argparse.ArgumentParser(description=description)

    # Required arguments
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Path to directory containing data files",
    )

    # Configuration files
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

    # Common session parameters
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

    # Output options
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to write output file to (defaults to data directory)",
    )

    return parser


def run_command(command: List[str]) -> int:
    """Run a command and return the exit code.

    Args:
        command: Command to run

    Returns:
        Exit code of the command
    """
    print(f"Running command: {' '.join(command)}")
    result = subprocess.run(command)
    return result.returncode


def resolve_parameters(args, required_params: List[str]) -> Dict[str, Any]:
    """Resolve parameters from command line args and config files.

    Args:
        args: Command line arguments
        required_params: List of required parameter names

    Returns:
        Dictionary of resolved parameters
    """
    # Start with empty parameters
    params = {}

    # Load session parameters config if specified
    if hasattr(args, "session_params") and args.session_params:
        session_config = load_config(args.session_params)
        params.update(session_config)

    # Load data streams config if specified
    data_streams = []
    if hasattr(args, "data_streams") and args.data_streams:
        data_streams = load_config(args.data_streams)

        # Handle the case where the file is empty or not a list
        if not isinstance(data_streams, list):
            print(
                f"Warning: Data streams file {args.data_streams} does not contain a valid list. Using empty list."
            )
            data_streams = []

    # Override with command line arguments (only if not None)
    cmd_args = vars(args)
    for key, value in cmd_args.items():
        if value is not None and key not in ["session_params", "data_streams"]:
            # Convert kebab-case to snake_case for parameter names
            param_key = key.replace("-", "_")

            # Convert active_mouse_platform string to boolean
            if param_key == "active_mouse_platform" and value is not None:
                value = value.lower() == "true"

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
    missing_params = [
        param for param in required_params if param not in params
    ]

    if missing_params:
        print(
            f"Error: Missing required parameters: {', '.join(missing_params)}"
        )
        sys.exit(1)

    return params


def run_etl(args, required_params: List[str], etl_class, job_settings_class):
    """Run an ETL process with the given arguments.

    Args:
        args: Command line arguments
        required_params: List of required parameter names
        etl_class: ETL class to instantiate
        job_settings_class: JobSettings class to instantiate

    Returns:
        None
    """
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

    # Debug: Print params before creating JobSettings
    print("\nDebug - Parameters being passed to JobSettings:")
    for key, value in params.items():
        print(f"  {key}: {value}")
    print(f"JobSettings class: {job_settings_class.__name__}")
    print(
        f"job_settings_name in class: {getattr(job_settings_class, 'job_settings_name', 'Not found')}"
    )

    # Create JobSettings object
    job_settings = job_settings_class(**params)

    # Generate session metadata
    etl = etl_class(job_settings)
    response = etl.run_job()

    # Handle the response
    if response.message:
        print(f"Response message: {response.message}")

    if response.data:
        # If we have data in the response, write it to the output file
        output_path = os.path.join(
            params["output_directory"], params["output_filename"]
        )
        with open(output_path, "w") as f:
            f.write(response.data)
        print(f"Session metadata saved to: {output_path}")
    else:
        # If no data in response, the file was likely written directly by the ETL process
        print(
            f"Session metadata saved to: {os.path.join(params['output_directory'], params['output_filename'])}"
        )
