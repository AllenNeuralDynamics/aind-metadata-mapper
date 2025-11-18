#!/usr/bin/env python3
"""Extract FIP metadata to ProtoAcquisitionDataSchema for subject 804434.

This script is for testing/development only. In production, the acquisition system
automatically generates fip.json (ProtoAcquisitionDataSchema) in the data directory.
"""

import json
from pathlib import Path

import aind_metadata_extractor
import jsonschema
from aind_physiology_fip.data_mappers import ProtoAcquisitionMapper

data_path = "/allen/aind/stage/vr-foraging/data/804434/804434_2025-11-05T014006Z"
# Write to the same directory as this script (dev_files/) to avoid writing to data directory
script_dir = Path(__file__).parent
output_path = script_dir / "fip_804434_2025-11-05T014006Z.json"

print(f"Extracting metadata from FIP data from: {data_path}")
acquisition_mapped = ProtoAcquisitionMapper(data_path).map()

print(f"Writing output to: {output_path}")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(acquisition_mapped.model_dump_json(indent=2))

print(f"Successfully extracted metadata and created fip.json to {output_path}")

# Validate against JSON schema
schema = json.load(open(Path(aind_metadata_extractor.__file__).parent / "models" / "fip.json"))
jsonschema.validate(instance=json.loads(acquisition_mapped.model_dump_json()), schema=schema)
print("Validation passed")
