#!/usr/bin/env python3
"""Combine FIP and behavior instrument metadata.

This script demonstrates combining two instruments into one:
1. Reads behavior instrument (instrument_behavior.json) - already schema-compliant
2. Reads FIP instrument (instrument_fip.json) - already schema-compliant
3. Combines both using the + operator (writes instrument_combined.json)

Files used from fip/:
- instrument_behavior.json: Behavior rig instrument
- instrument_fip.json: FIP rig instrument
- instrument_combined.json: Combined FIP + behavior instrument (output)

Environment Setup:
    conda create -n fip-mapper python=3.11
    conda activate fip-mapper
    pip install -e .

Usage:
    cd src/aind_metadata_mapper/fip
    python combine_instruments.py
"""

import json
from pathlib import Path

from aind_data_schema.core.instrument import Instrument


def main():
    """Combine FIP and behavior instrument metadata."""
    fip_dir = Path(__file__).parent

    # Load behavior instrument
    behavior_path = fip_dir / "instrument_behavior.json"
    print(f"Reading behavior instrument from: {behavior_path}")
    with open(behavior_path) as f:
        behavior_instrument_dict = json.load(f)
    behavior_instrument = Instrument.model_validate(behavior_instrument_dict)

    # Load FIP instrument
    fip_path = fip_dir / "instrument_fip.json"
    print(f"Reading FIP instrument from: {fip_path}")
    with open(fip_path) as f:
        fip_instrument_dict = json.load(f)
    fip_instrument = Instrument.model_validate(fip_instrument_dict)

    # Align FIP instrument fields to match behavior instrument
    fip_instrument.instrument_id = behavior_instrument.instrument_id
    fip_instrument.location = behavior_instrument.location

    # Combine
    combined_instrument = behavior_instrument + fip_instrument

    # Write combined instrument
    output_path = fip_dir / "instrument_combined.json"
    with open(output_path, "w") as f:
        f.write(combined_instrument.model_dump_json(indent=2))
    print(f"Combined instrument written to: {output_path}")


if __name__ == "__main__":
    main()
