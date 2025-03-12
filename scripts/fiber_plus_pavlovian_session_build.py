#!/usr/bin/env python3
"""Script for generating combined Fiber Photometry and Pavlovian Behavior session metadata."""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate combined Fiber Photometry and Pavlovian Behavior session metadata"
    )

    # Required arguments
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Path to directory containing data files",
    )

    # Session parameters
    parser.add_argument(
        "--session-params",
        type=str,
        required=True,
        help="Path to combined session parameters JSON file",
    )

    # Data streams
    parser.add_argument(
        "--fiber-data-streams",
        type=str,
        help="Path to Fiber Photometry data streams JSON file",
    )
    parser.add_argument(
        "--pavlovian-data-streams",
        type=str,
        help="Path to Pavlovian behavior data streams JSON file",
    )

    # Output options
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directory to write output files to (defaults to data directory)",
    )
    parser.add_argument(
        "--combined-output-filename",
        type=str,
        default="session_combined.json",
        help="Name of the combined output file",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output files if they exist",
    )

    return parser.parse_args()


def run_command(command):
    """Run a command and return the exit code."""
    print(f"Running command: {command}")
    result = subprocess.run(command, shell=True)
    return result.returncode


def merge_json_files(file1, file2, output_file):
    """Merge two JSON files into one.

    For keys that are strings, the value from file1 is used.
    For keys that are lists, the lists are combined.
    """
    with open(file1, "r") as f1:
        data1 = json.load(f1)

    with open(file2, "r") as f2:
        data2 = json.load(f2)

    # Start with data1 as the base
    merged_data = data1.copy()

    # Merge data from data2
    for key, value in data2.items():
        if key not in merged_data:
            # If key doesn't exist in merged_data, just add it
            merged_data[key] = value
        elif isinstance(value, list) and isinstance(merged_data[key], list):
            # If both are lists, combine them
            # Only works for simple types like strings
            if all(isinstance(item, str) for item in merged_data[key] + value):
                merged_data[key] = list(set(merged_data[key] + value))
            else:
                # For complex objects, just append
                merged_data[key] = merged_data[key] + value
        # If both are dictionaries, we could recursively merge them
        # For now, we'll keep the value from data1

    # Write the merged data to the output file
    with open(output_file, "w") as f:
        json.dump(merged_data, f, indent=2)

    print(f"Merged JSON saved to: {output_file}")


def create_modality_specific_params(combined_params, modality):
    """Create modality-specific parameters from combined parameters.

    Args:
        combined_params: Dictionary containing combined parameters
        modality: String indicating which modality to extract ('fiber_photometry' or 'pavlovian_behavior')

    Returns:
        Dictionary with modality-specific parameters
    """
    # Start with common parameters
    params = {
        k: v
        for k, v in combined_params.items()
        if k not in ["fiber_photometry", "pavlovian_behavior"]
    }

    # Add modality-specific parameters
    if modality in combined_params:
        for k, v in combined_params[modality].items():
            params[k] = v

    return params


def main():
    """Main entry point for the script."""
    args = parse_args()

    # Set output directory
    output_dir = args.output_dir or args.data_dir

    # Define output filenames
    fiber_output = os.path.join(output_dir, "session_fiber_photometry.json")
    pavlovian_output = os.path.join(
        output_dir, "session_pavlovian_behavior.json"
    )
    combined_output = os.path.join(output_dir, args.combined_output_filename)

    # Check if output files exist
    if not args.overwrite and os.path.exists(combined_output):
        print(
            f"Error: Output file {combined_output} already exists. Use --overwrite to overwrite."
        )
        sys.exit(1)

    # Load combined parameters
    with open(args.session_params, "r") as f:
        combined_params = json.load(f)

    # Create temporary files for each modality
    fiber_params = create_modality_specific_params(
        combined_params, "fiber_photometry"
    )
    pavlovian_params = create_modality_specific_params(
        combined_params, "pavlovian_behavior"
    )

    # Write temporary files
    fiber_params_file = os.path.join(output_dir, "_temp_fiber_params.json")
    with open(fiber_params_file, "w") as f:
        json.dump(fiber_params, f)

    pavlovian_params_file = os.path.join(
        output_dir, "_temp_pavlovian_params.json"
    )
    with open(pavlovian_params_file, "w") as f:
        json.dump(pavlovian_params, f)

    # Run Fiber Photometry script
    print("\n=== Generating Fiber Photometry Session Metadata ===\n")

    # Build fiber command
    fiber_cmd = "python scripts/fiber_photometry_session_build.py"
    fiber_cmd += f" --data-dir {args.data_dir}"
    if args.output_dir:
        fiber_cmd += f" --output-dir {args.output_dir}"
    fiber_cmd += f" --session-params {fiber_params_file}"
    if args.fiber_data_streams:
        fiber_cmd += f" --data-streams {args.fiber_data_streams}"

    exit_code = run_command(fiber_cmd)
    if exit_code != 0:
        print(
            f"Error: Fiber photometry script failed with exit code {exit_code}"
        )
        # Clean up temporary files
        if os.path.exists(fiber_params_file):
            os.remove(fiber_params_file)
        if os.path.exists(pavlovian_params_file):
            os.remove(pavlovian_params_file)
        sys.exit(exit_code)

    # Run Pavlovian Behavior script
    print("\n=== Generating Pavlovian Behavior Session Metadata ===\n")

    # Determine behavior directory
    behavior_dir = os.path.join(args.data_dir, "behavior")
    if os.path.exists(behavior_dir):
        pavlovian_data_dir = behavior_dir
    else:
        pavlovian_data_dir = args.data_dir

    # Build pavlovian command
    pavlovian_cmd = "python scripts/pavlovian_session_build.py"
    pavlovian_cmd += f" --data-dir {pavlovian_data_dir}"
    if args.output_dir:
        pavlovian_cmd += f" --output-dir {args.output_dir}"
    pavlovian_cmd += f" --session-params {pavlovian_params_file}"
    if args.pavlovian_data_streams:
        pavlovian_cmd += f" --data-streams {args.pavlovian_data_streams}"

    exit_code = run_command(pavlovian_cmd)
    if exit_code != 0:
        print(
            f"Error: Pavlovian behavior script failed with exit code {exit_code}"
        )
        # Clean up temporary files
        if os.path.exists(fiber_params_file):
            os.remove(fiber_params_file)
        if os.path.exists(pavlovian_params_file):
            os.remove(pavlovian_params_file)
        sys.exit(exit_code)

    # Merge the two JSON files
    print("\n=== Merging Session Metadata Files ===\n")
    merge_json_files(fiber_output, pavlovian_output, combined_output)

    # Clean up temporary files
    if os.path.exists(fiber_params_file):
        os.remove(fiber_params_file)
    if os.path.exists(pavlovian_params_file):
        os.remove(pavlovian_params_file)

    print(f"\nCombined session metadata saved to: {combined_output}")
    print(f"Individual session files saved to:")
    print(f"  - Fiber photometry: {fiber_output}")
    print(f"  - Pavlovian behavior: {pavlovian_output}")


if __name__ == "__main__":
    main()
