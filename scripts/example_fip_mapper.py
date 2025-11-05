"""Example script demonstrating FIP mapper with ProtoAcquisitionDataSchema JSON validation.

This script demonstrates the complete workflow:
1. Load ProtoAcquisitionDataSchema JSON file from acquisition repo
2. Validate against JSON schema from extractor repo
3. Map to AIND Data Schema 2.0 Acquisition format

Usage:
    python scripts/example_fip_mapper.py /path/to/ProtoAcquisitionDataSchema.json

Example:
    python scripts/example_fip_mapper.py /Users/doug.ollerenshaw/code/Aind.Physiology.Fip/examples/ProtoAcquisitionDataSchema.json
"""

import argparse
import json
import sys
from pathlib import Path

from aind_metadata_mapper.fip.mapper import FIPMapper


def main():
    parser = argparse.ArgumentParser(
        description="Demonstrate FIP mapper workflow: validate and transform ProtoAcquisitionDataSchema to AIND Data Schema 2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python scripts/example_fip_mapper.py /path/to/ProtoAcquisitionDataSchema.json

Need an example input file?
  See: https://github.com/AllenNeuralDynamics/Aind.Physiology.Fip/blob/main/examples/ProtoAcquisitionDataSchema.json
        """,
    )
    parser.add_argument(
        "input_json",
        type=Path,
        help="Path to ProtoAcquisitionDataSchema JSON file",
    )

    args = parser.parse_args()
    example_path = args.input_json

    # Validate that the file exists
    if not example_path.exists():
        print(f"Error: File not found: {example_path}")
        print("\nNeed an example input file?")
        print(
            "See: https://github.com/AllenNeuralDynamics/Aind.Physiology.Fip/blob/main/examples/ProtoAcquisitionDataSchema.json"
        )
        return 1

    print("=" * 60)
    print("FIP Mapper Example")
    print("=" * 60)

    # Load the example JSON
    print(f"\n1. Loading example JSON from:\n   {example_path}")
    with open(example_path) as f:
        metadata = json.load(f)

    print(f"   ✓ Loaded JSON with keys: {list(metadata.keys())}")

    # Create mapper
    print("\n2. Creating FIP mapper...")
    mapper = FIPMapper()
    print("   ✓ Mapper initialized")

    # Validate and transform
    print("\n3. Validating input and transforming to AIND Data Schema 2.0...")
    print("   (Validating against JSON schema from aind-metadata-extractor)")
    try:
        acquisition = mapper.transform(metadata)
        print("   ✓ Validation passed and transformation complete!")

        # Display results
        print("\n4. Acquisition metadata created:")
        print(f"   - Subject ID: {acquisition.subject_id}")
        print(f"   - Instrument ID: {acquisition.instrument_id}")
        print(f"   - Acquisition type: {acquisition.acquisition_type}")
        print(f"   - Start time: {acquisition.acquisition_start_time}")
        print(f"   - End time: {acquisition.acquisition_end_time}")
        print(f"   - Experimenters: {', '.join(acquisition.experimenters)}")
        print(f"   - Data streams: {len(acquisition.data_streams)}")

        if acquisition.data_streams:
            stream = acquisition.data_streams[0]
            print(f"   - Active devices: {len(stream.active_devices)}")
            print(f"   - Configurations: {len(stream.configurations)}")

        # Optionally write output
        output_file = mapper.write(acquisition, output_directory=".")
        print(f"\n5. Wrote output to: {output_file.absolute()}")

    except ValueError as e:
        print(f"\n   ✗ Validation failed: {e}")
        return 1
    except Exception as e:
        print(f"\n   ✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
