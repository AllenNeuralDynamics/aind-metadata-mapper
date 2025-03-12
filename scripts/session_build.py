#!/usr/bin/env python3
"""Script for generating session metadata for multiple modalities.

This script reads a session parameters file that includes a 'session_types' list
and dynamically imports the appropriate ETL classes for each modality.
"""

import importlib
import json
import os
import sys
from pathlib import Path

from aind_data_schema_models.modalities import Modality
from aind_metadata_mapper.utils.cli import create_common_parser


def parse_args():
    """Parse command line arguments."""
    parser = create_common_parser(
        "Generate session metadata for multiple modalities"
    )

    parser.add_argument(
        "--output-filename",
        type=str,
        default="session.json",
        help="Name of the output file (defaults to session.json)",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output files if they exist",
    )

    return parser.parse_args()


def main():
    """Main entry point for the script."""
    args = parse_args()

    # Load session parameters
    try:
        with open(args.session_params, "r") as f:
            session_params = json.load(f)
    except FileNotFoundError:
        print(
            f"Error: Session parameters file '{args.session_params}' not found."
        )
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(
            f"Error: Invalid JSON in session parameters file '{args.session_params}'."
        )
        print(f"JSON error: {e}")
        print("Make sure the file contains valid JSON without comments.")
        sys.exit(1)

    # Check for session_types
    if "session_types" not in session_params:
        print("Error: session_types list not found in session parameters file")
        sys.exit(1)

    session_types = session_params["session_types"]
    if not session_types:
        print("Error: session_types list is empty")
        sys.exit(1)

    # Set output directory
    output_dir = args.output_dir or args.data_dir
    os.makedirs(output_dir, exist_ok=True)

    # Define output filename
    merged_file = os.path.join(output_dir, args.output_filename)

    # Check if output file exists and overwrite flag is not set
    if not args.overwrite and os.path.exists(merged_file):
        print(
            f"Error: Output file {merged_file} already exists. Use --overwrite to overwrite."
        )
        sys.exit(1)

    # Process each modality
    output_files = []

    for modality in session_types:
        print(f"\n=== Processing {modality} ===")

        # Create a copy of the parameters without session_types
        params = session_params.copy()
        if "session_types" in params:
            del params["session_types"]

        # Dynamically import the ETL class and JobSettings for this modality
        try:
            # Import the module
            session_module = importlib.import_module(
                f"aind_metadata_mapper.{modality}.session"
            )
            models_module = importlib.import_module(
                f"aind_metadata_mapper.{modality}.models"
            )

            # Get the ETL class and JobSettings class
            etl_class = session_module.ETL
            job_settings_class = models_module.JobSettings

            # Set data directory based on modality
            data_dir = args.data_dir

            # Check for modality-specific subdirectory (without if statements)
            modality_subdirs = {
                "pavlovian_behavior": "behavior",
                # Add other modalities and their subdirectories here
            }
            subdir = modality_subdirs.get(modality, "")
            modality_dir = os.path.join(args.data_dir, subdir)
            data_dir = (
                modality_dir
                if subdir and os.path.exists(modality_dir)
                else args.data_dir
            )

            # Set session_type based on modality (with proper formatting)
            session_type_map = {
                "fiber_photometry": "Fiber_Photometry",
                "pavlovian_behavior": "PavlovianConditioning",
                # Add other modalities and their session types here
            }
            params["session_type"] = session_type_map.get(
                modality, modality.replace("_", " ").title().replace(" ", "_")
            )

            # Load data streams if provided
            # First check for modality-specific data streams file
            modality_data_streams_file = None
            if args.data_streams:
                # Check if the provided file is modality-specific
                if modality in args.data_streams:
                    modality_data_streams_file = args.data_streams
                else:
                    # Try to find a modality-specific file in the same directory
                    data_streams_dir = os.path.dirname(args.data_streams)
                    data_streams_name = f"{modality}_data_streams.json"
                    potential_file = os.path.join(
                        data_streams_dir, data_streams_name
                    )
                    if os.path.exists(potential_file):
                        modality_data_streams_file = potential_file
                    else:
                        # Fall back to the provided file
                        modality_data_streams_file = args.data_streams

            if modality_data_streams_file:
                try:
                    with open(modality_data_streams_file, "r") as f:
                        data_streams = json.load(f)

                        # Map modalities to their corresponding stream_modalities values
                        modality_to_stream = {
                            "fiber_photometry": Modality.FIB,
                            "pavlovian_behavior": Modality.BEHAVIOR,
                            # Add other modalities here
                        }

                        # Update stream_modalities for each stream based on current modality
                        stream_modality = modality_to_stream.get(
                            modality,
                            getattr(
                                Modality, modality.upper(), modality.upper()
                            ),
                        )
                        for stream in data_streams:
                            stream["stream_modalities"] = [stream_modality]

                        params["data_streams"] = data_streams
                        print(
                            f"Using data streams from: {modality_data_streams_file}"
                        )
                except FileNotFoundError:
                    print(
                        f"Error: Data streams file '{modality_data_streams_file}' not found."
                    )
                    continue
                except json.JSONDecodeError as e:
                    print(
                        f"Error: Invalid JSON in data streams file '{modality_data_streams_file}'."
                    )
                    print(f"JSON error: {e}")
                    print(
                        "Make sure the file contains valid JSON without comments."
                    )
                    continue

            # Set paths
            params["data_directory"] = data_dir
            params["output_directory"] = output_dir
            output_filename = f"session_{modality}.json"
            output_path = os.path.join(output_dir, output_filename)

            # Check if output file exists and overwrite flag is not set
            if not args.overwrite and os.path.exists(output_path):
                print(
                    f"Error: Output file {output_path} already exists. Use --overwrite to overwrite."
                )
                continue

            params["output_filename"] = output_filename

            # Create job settings and run ETL
            job_settings = job_settings_class(**params)
            etl = etl_class(job_settings)
            response = etl.run_job()

            if response.status_code != 200:
                print(
                    f"Warning: {modality} ETL process returned status code {response.status_code}"
                )
                if response.message:
                    print(f"Message: {response.message}")
            else:
                output_files.append(os.path.join(output_dir, output_filename))

        except Exception as e:
            print(f"Error processing {modality}: {str(e)}")
            continue

    # Merge output files if more than one
    if len(output_files) > 1:
        print("\n=== Merging output files ===")

        # Start with the first file
        merged_file = os.path.join(output_dir, args.output_filename)

        # Merge files
        with open(output_files[0], "r") as f:
            merged_data = json.load(f)

        for file_path in output_files[1:]:
            with open(file_path, "r") as f:
                data = json.load(f)

            # Merge data
            for key, value in data.items():
                if key not in merged_data:
                    merged_data[key] = value
                elif isinstance(value, list) and isinstance(
                    merged_data[key], list
                ):
                    # For simple types, remove duplicates
                    if all(
                        isinstance(item, str)
                        for item in merged_data[key] + value
                    ):
                        merged_data[key] = list(set(merged_data[key] + value))
                    else:
                        # For complex objects, just append
                        merged_data[key] = merged_data[key] + value

        # Write merged file
        with open(merged_file, "w") as f:
            json.dump(merged_data, f, indent=2)

        print(f"Combined session metadata saved to: {merged_file}")
    elif len(output_files) == 1:
        # Just copy the single file to the output file
        merged_file = os.path.join(output_dir, args.output_filename)
        with open(output_files[0], "r") as src:
            with open(merged_file, "w") as dst:
                dst.write(src.read())
        print(f"Session metadata saved to: {merged_file}")


if __name__ == "__main__":
    main()
