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

    # Fiber photometry specific arguments
    parser.add_argument(
        "--fiber-session-params",
        type=str,
        help="Path to Fiber Photometry session parameters JSON file",
    )
    parser.add_argument(
        "--fiber-data-streams",
        type=str,
        help="Path to Fiber Photometry data streams JSON file",
    )

    # Pavlovian behavior specific arguments
    parser.add_argument(
        "--pavlovian-session-params",
        type=str,
        help="Path to Pavlovian behavior session parameters JSON file",
    )
    parser.add_argument(
        "--pavlovian-data-streams",
        type=str,
        help="Path to Pavlovian behavior data streams JSON file",
    )

    # Common arguments that can be shared
    parser.add_argument(
        "--session-params",
        type=str,
        help="Path to common session parameters JSON file (will be used if modality-specific params not provided)",
    )
    parser.add_argument(
        "--data-streams",
        type=str,
        help="Path to common data streams JSON file (will be used if modality-specific streams not provided)",
    )

    # Common parameters that can be passed to both scripts
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
        "--iacuc-protocol",
        type=str,
        help="IACUC protocol identifier",
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
            # Remove duplicates by converting to a set and back to a list
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

    # Build Fiber Photometry command
    fiber_cmd = f"python scripts/fiber_photometry_session_build.py --data-dir {args.data_dir}"

    if args.output_dir:
        fiber_cmd += f" --output-dir {args.output_dir}"

    # Add fiber-specific parameters
    if args.fiber_session_params:
        fiber_cmd += f" --session-params {args.fiber_session_params}"
    elif args.session_params:
        fiber_cmd += f" --session-params {args.session_params}"

    if args.fiber_data_streams:
        fiber_cmd += f" --data-streams {args.fiber_data_streams}"
    elif args.data_streams:
        fiber_cmd += f" --data-streams {args.data_streams}"

    # Add common parameters if provided
    if args.subject_id:
        fiber_cmd += f" --subject-id {args.subject_id}"
    if args.experimenter_full_name:
        fiber_cmd += (
            f' --experimenter-full-name "{args.experimenter_full_name}"'
        )
    if args.rig_id:
        fiber_cmd += f" --rig-id {args.rig_id}"
    if args.iacuc_protocol:
        fiber_cmd += f" --iacuc-protocol {args.iacuc_protocol}"

    # Run Fiber Photometry script
    print("\n=== Generating Fiber Photometry Session Metadata ===\n")
    exit_code = run_command(fiber_cmd)
    if exit_code != 0:
        print(
            f"Error: Fiber photometry script failed with exit code {exit_code}"
        )
        sys.exit(exit_code)

    # Build Pavlovian behavior command
    behavior_dir = os.path.join(args.data_dir, "behavior")
    if os.path.exists(behavior_dir):
        pavlovian_data_dir = behavior_dir
    else:
        pavlovian_data_dir = args.data_dir

    pavlovian_cmd = f"python scripts/pavlovian_session_build.py --data-dir {pavlovian_data_dir}"

    if args.output_dir:
        pavlovian_cmd += f" --output-dir {args.output_dir}"

    # Add pavlovian-specific parameters
    if args.pavlovian_session_params:
        pavlovian_cmd += f" --session-params {args.pavlovian_session_params}"
    elif args.session_params:
        pavlovian_cmd += f" --session-params {args.session_params}"

    if args.pavlovian_data_streams:
        pavlovian_cmd += f" --data-streams {args.pavlovian_data_streams}"
    elif args.data_streams:
        pavlovian_cmd += f" --data-streams {args.data_streams}"

    # Add common parameters if provided
    if args.subject_id:
        pavlovian_cmd += f" --subject-id {args.subject_id}"
    if args.experimenter_full_name:
        pavlovian_cmd += (
            f' --experimenter-full-name "{args.experimenter_full_name}"'
        )
    if args.rig_id:
        pavlovian_cmd += f" --rig-id {args.rig_id}"
    if args.iacuc_protocol:
        pavlovian_cmd += f" --iacuc-protocol {args.iacuc_protocol}"

    # Run Pavlovian behavior script
    print("\n=== Generating Pavlovian Behavior Session Metadata ===\n")
    exit_code = run_command(pavlovian_cmd)
    if exit_code != 0:
        print(
            f"Error: Pavlovian behavior script failed with exit code {exit_code}"
        )
        sys.exit(exit_code)

    # Merge the two JSON files
    print("\n=== Merging Session Metadata Files ===\n")
    merge_json_files(fiber_output, pavlovian_output, combined_output)

    print(f"\nCombined session metadata saved to: {combined_output}")
    print(f"Individual session files saved to:")
    print(f"  - Fiber photometry: {fiber_output}")
    print(f"  - Pavlovian behavior: {pavlovian_output}")


if __name__ == "__main__":
    main()
