"""Command-line interface utilities for AIND metadata mapper."""

import json
import sys
from typing import Dict, Any, List


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
