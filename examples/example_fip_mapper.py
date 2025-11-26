"""Example script demonstrating FIP mapper with extracted metadata.

This script demonstrates the complete workflow:
1. Read FIP extracted metadata (fip.json) - raw extracted data, not yet mapped to schema
2. Map FIP metadata to a schema-compliant acquisition (writes acquisition.json)

Usage:
    python examples/example_fip_mapper.py --input-path /path/to/fip.json [--output-file output_filename]

Example:
    python examples/example_fip_mapper.py --input-path /path/to/fip.json
    python examples/example_fip_mapper.py --input-path /path/to/fip.json --output-file example_acquisition.json
"""

import argparse
import json
import logging
from pathlib import Path

from aind_metadata_mapper.fip.mapper import FIPMapper

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


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
  python examples/example_fip_mapper.py --input-path /path/to/fip.json [--output-file output_filename]

  # Use default output filename (acquisition.json)
  python examples/example_fip_mapper.py --input-path /path/to/fip.json

  # Specify custom output filename
  python examples/example_fip_mapper.py --input-path /path/to/fip.json --output-file example_acquisition.json
        """,
    )
    parser.add_argument(
        "--input-path",
        type=Path,
        required=True,
        help="Path to extracted FIP metadata JSON file (fip.json)",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="acquisition.json",
        help="Output filename (default: acquisition.json)",
    )

    args = parser.parse_args()
    fip_path = args.input_path
    output_filename = args.output_file

    # Validate that the file exists
    if not fip_path.exists():
        logger.error(f"File not found: {fip_path}")
        return 1

    logger.info("=" * 60)
    logger.info("FIP Mapper Example")
    logger.info("=" * 60)

    # Load the extracted FIP metadata
    logger.info(f"\n1. Loading extracted FIP metadata from:\n   {fip_path}")
    with open(fip_path) as f:
        fip_metadata = json.load(f)

    logger.info(f"   Loaded JSON with keys: {list(fip_metadata.keys())}")

    # Create mapper
    logger.info(f"\n2. Creating FIP mapper (output: {output_filename})...")
    mapper = FIPMapper(output_filename=output_filename)
    logger.info("   Mapper initialized")

    # Transform to schema-compliant acquisition
    logger.info("\n3. Mapping FIP metadata to AIND Data Schema 2.0 Acquisition...")
    try:
        acquisition = mapper.transform(fip_metadata, skip_validation=True)
        logger.info("   Transformation complete!")

        # Display results
        logger.info("\n4. Acquisition metadata created:")
        logger.info(f"   - Subject ID: {acquisition.subject_id}")
        logger.info(f"   - Instrument ID: {acquisition.instrument_id}")
        logger.info(f"   - Acquisition type: {acquisition.acquisition_type}")
        logger.info(f"   - Start time: {acquisition.acquisition_start_time}")
        logger.info(f"   - End time: {acquisition.acquisition_end_time}")
        logger.info(f"   - Experimenters: {', '.join(acquisition.experimenters)}")
        logger.info(f"   - Data streams: {len(acquisition.data_streams)}")

        if acquisition.data_streams:
            stream = acquisition.data_streams[0]
            logger.info(f"   - Active devices: {len(stream.active_devices)}")
            logger.info(f"   - Configurations: {len(stream.configurations)}")

        # Write output to examples folder using write_standard_file
        examples_dir = Path(__file__).parent
        # Extract suffix from filename if custom (e.g., "example_acquisition.json" -> "_example")
        output_filename_stem = Path(mapper.output_filename).stem  # Remove .json extension
        if output_filename_stem == "acquisition":
            suffix = None
        else:
            # Extract suffix from filename like "example_acquisition" -> "_example"
            # Split on "_acquisition" to get the prefix
            if "_acquisition" in output_filename_stem:
                prefix = output_filename_stem.split("_acquisition")[0]
                suffix = "_" + prefix if prefix else None
            else:
                suffix = None

        acquisition.write_standard_file(output_directory=examples_dir, suffix=suffix)
        # write_standard_file creates acquisition.json or acquisition{suffix}.json
        if suffix:
            output_file = examples_dir / f"acquisition{suffix}.json"
        else:
            output_file = examples_dir / "acquisition.json"
        logger.info(f"\n5. Wrote output to: {output_file.absolute()}")

    except ValueError as e:
        logger.error(f"\n   Error: {e}")
        return 1
    except Exception as e:
        logger.error(f"\n   Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
