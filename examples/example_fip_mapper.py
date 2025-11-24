"""Example script demonstrating FIP mapper with extracted metadata.

This script demonstrates the complete workflow:
1. Read FIP extracted metadata (fip.json) - raw extracted data, not yet mapped to schema
2. Map FIP metadata to a schema-compliant acquisition (writes acquisition.json)

Usage:
    python examples/example_fip_mapper.py /path/to/fip.json [output_filename]

Example:
    python examples/example_fip_mapper.py /path/to/fip.json
    python examples/example_fip_mapper.py /path/to/fip.json example_acquisition.json
"""

import argparse
import json
from pathlib import Path

from aind_metadata_mapper.fip.mapper import FIPMapper
from aind_metadata_mapper.utils import write_acquisition


def main():
    """Main entry point for the FIP mapper example script.

    Parses command line arguments, loads the extracted FIP metadata file,
    transforms it using the FIP mapper, and writes the output acquisition metadata file.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Demonstrate FIP mapper workflow: transform extracted FIP metadata "
            "to AIND Data Schema 2.0 Acquisition format"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python examples/example_fip_mapper.py /path/to/fip.json [output_filename]

  # Use default output filename (acquisition.json)
  python examples/example_fip_mapper.py /path/to/fip.json

  # Specify custom output filename
  python examples/example_fip_mapper.py /path/to/fip.json example_acquisition.json
        """,
    )
    parser.add_argument(
        "fip_json",
        type=Path,
        help="Path to extracted FIP metadata JSON file (fip.json)",
    )
    parser.add_argument(
        "output_filename",
        type=str,
        nargs="?",
        default="acquisition.json",
        help="Output filename (default: acquisition.json)",
    )

    args = parser.parse_args()
    fip_path = args.fip_json
    output_filename = args.output_filename

    # Validate that the file exists
    if not fip_path.exists():
        print(f"Error: File not found: {fip_path}")
        return 1

    print("=" * 60)
    print("FIP Mapper Example")
    print("=" * 60)

    # Load the extracted FIP metadata
    print(f"\n1. Loading extracted FIP metadata from:\n   {fip_path}")
    with open(fip_path) as f:
        fip_metadata = json.load(f)

    print(f"   ✓ Loaded JSON with keys: {list(fip_metadata.keys())}")

    # Create mapper
    print(f"\n2. Creating FIP mapper (output: {output_filename})...")
    mapper = FIPMapper(output_filename=output_filename)
    print("   ✓ Mapper initialized")

    # Transform to schema-compliant acquisition
    print("\n3. Mapping FIP metadata to AIND Data Schema 2.0 Acquisition...")
    try:
        acquisition = mapper.transform(fip_metadata, skip_validation=True)
        print("   ✓ Transformation complete!")

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

        # Write output to examples folder
        examples_dir = Path(__file__).parent
        # Use just the filename if output_filename contains a path
        output_filename = Path(mapper.output_filename).name
        output_file = write_acquisition(acquisition, str(examples_dir), output_filename)
        print(f"\n5. Wrote output to: {output_file.absolute()}")

    except ValueError as e:
        print(f"\n   ✗ Error: {e}")
        return 1
    except Exception as e:
        print(f"\n   ✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
