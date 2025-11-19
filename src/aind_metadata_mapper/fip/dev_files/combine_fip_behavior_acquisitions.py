#!/usr/bin/env python3
"""Combine FIP and behavior acquisition metadata.

This script demonstrates combining two acquisitions into one:
1. Reads behavior acquisition (acquisition.json) - already mapped to aind-data-schema
2. Reads FIP extracted metadata (fip.json) - raw extracted data, not yet mapped to schema
3. Maps FIP metadata to a schema-compliant acquisition (writes acquisition_fip.json)
4. Combines both schema-compliant acquisitions using the + operator (writes acquisition_combined.json)

Files created in dev_files/:
- acquisition_fip.json: FIP acquisition after mapping
- acquisition_combined.json: Combined FIP + behavior acquisition

Environment Setup:
    conda create -n fip-mapper python=3.11
    conda activate fip-mapper
    pip install -e .

Usage:
    cd src/aind_metadata_mapper/fip/dev_files
    python combine_fip_behavior_acquisitions.py /path/to/data/directory

Example with real data:
    cd src/aind_metadata_mapper/fip/dev_files
    python combine_fip_behavior_acquisitions.py /allen/aind/stage/vr-foraging/data/804434/804434_2025-11-14T010241Z
"""

import argparse
import json
from pathlib import Path

from aind_data_schema.core.acquisition import Acquisition

from aind_metadata_mapper.fip.mapper import FIPMapper


def main():
    """Combine FIP and behavior acquisitions."""
    parser = argparse.ArgumentParser(description="Combine FIP and behavior acquisition metadata")
    parser.add_argument("data_directory", help="Path to data directory with fip.json and acquisition.json")
    args = parser.parse_args()

    data_dir = Path(args.data_directory)

    # Read behavior acquisition
    behavior_path = data_dir / "acquisition.json"
    print(f"Reading behavior acquisition from: {behavior_path}")
    with open(behavior_path) as f:
        behavior_acquisition_dict = json.load(f)

    # Read extracted FIP data
    fip_path = data_dir / "fip.json"
    print(f"Reading extracted FIP data from: {fip_path}")
    with open(fip_path) as f:
        fip_metadata = json.load(f)

    # Map FIP data to acquisition
    print("Mapping extracted FIP data to FIP acquisition...")
    mapper = FIPMapper()
    fip_acquisition = mapper.transform(fip_metadata, skip_validation=True)

    # Write intermediate FIP acquisition
    fip_output_path = Path(__file__).parent / "acquisition_fip.json"
    with open(fip_output_path, "w") as f:
        f.write(fip_acquisition.model_dump_json(indent=2))
    print(f"FIP acquisition written to: {fip_output_path}")

    # Load behavior acquisition
    behavior_acquisition = Acquisition.model_validate(behavior_acquisition_dict)

    # Combine
    combined_acquisition = behavior_acquisition + fip_acquisition

    # Write combined acquisition
    combined_output_path = Path(__file__).parent / "acquisition_combined.json"
    with open(combined_output_path, "w") as f:
        f.write(combined_acquisition.model_dump_json(indent=2))
    print(f"Combined acquisition written to: {combined_output_path}")


if __name__ == "__main__":
    main()
