#!/usr/bin/env python3
"""Script for generating combined Fiber Photometry and Pavlovian Behavior session metadata."""

import argparse
import os
import sys
import shlex
from pathlib import Path

from aind_metadata_mapper.utils.cli import run_command


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

    # Configuration files
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

    # Output options
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directory to write output files to (defaults to data directory)",
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_combined.json",
        help="Name of the combined output file",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output files if they exist",
    )

    # Pass through any other arguments to both scripts
    parser.add_argument(
        "--extra-args",
        type=str,
        help="Additional arguments to pass to both scripts (e.g. '--subject-id 123456 --experimenter-full-name \"John Doe\"')",
    )

    return parser.parse_args()


def main():
    """Main entry point for the script."""
    args = parse_args()

    # Set output directory
    output_dir = args.output_dir or args.data_dir

    # Define output filenames
    pavlovian_output = os.path.join(
        output_dir, "session_pavlovian_behavior.json"
    )
    fiber_output = os.path.join(output_dir, "session_fiber_photometry.json")
    combined_output = os.path.join(output_dir, args.output_filename)

    # Check if output files exist
    if not args.overwrite:
        for output_file in [pavlovian_output, fiber_output, combined_output]:
            if os.path.exists(output_file):
                print(
                    f"Error: Output file {output_file} already exists. Use --overwrite to overwrite."
                )
                sys.exit(1)

    # Build Pavlovian behavior command
    pavlovian_cmd = (
        f"python scripts/pavlovian_session_build.py --data-dir {args.data_dir}"
    )

    if args.pavlovian_session_params:
        pavlovian_cmd += f" --session-params {args.pavlovian_session_params}"
    if args.pavlovian_data_streams:
        pavlovian_cmd += f" --data-streams {args.pavlovian_data_streams}"
    if args.output_dir:
        pavlovian_cmd += f" --output-dir {args.output_dir}"
    if args.extra_args:
        pavlovian_cmd += f" {args.extra_args}"

    # Run Pavlovian behavior script
    print("\n=== Generating Pavlovian Behavior Session Metadata ===\n")
    exit_code = run_command(shlex.split(pavlovian_cmd))
    if exit_code != 0:
        print(
            f"Error: Pavlovian behavior script failed with exit code {exit_code}"
        )
        sys.exit(exit_code)

    # Build Fiber Photometry command
    fiber_cmd = f"python scripts/fiber_photometry_session_build.py --data-dir {args.data_dir}"

    if args.fiber_session_params:
        fiber_cmd += f" --session-params {args.fiber_session_params}"
    if args.fiber_data_streams:
        fiber_cmd += f" --data-streams {args.fiber_data_streams}"
    if args.output_dir:
        fiber_cmd += f" --output-dir {args.output_dir}"
    if args.extra_args:
        fiber_cmd += f" {args.extra_args}"

    # Run Fiber Photometry script
    print("\n=== Generating Fiber Photometry Session Metadata ===\n")
    exit_code = run_command(shlex.split(fiber_cmd))
    if exit_code != 0:
        print(
            f"Error: Fiber Photometry script failed with exit code {exit_code}"
        )
        sys.exit(exit_code)

    # Build merge command
    merge_cmd = f"python scripts/merge_session_files.py --input-files {pavlovian_output} {fiber_output} --output-file {combined_output}"
    if args.overwrite:
        merge_cmd += " --overwrite"

    # Run merge script
    print("\n=== Merging Session Metadata Files ===\n")
    exit_code = run_command(shlex.split(merge_cmd))
    if exit_code != 0:
        print(f"Error: Merge script failed with exit code {exit_code}")
        sys.exit(exit_code)

    print(f"\nCombined session metadata saved to: {combined_output}")
    print(f"Individual session files saved to:")
    print(f"  - Pavlovian behavior: {pavlovian_output}")
    print(f"  - Fiber photometry: {fiber_output}")


if __name__ == "__main__":
    main()
