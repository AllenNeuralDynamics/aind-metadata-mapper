#!/usr/bin/env python3
"""Map FIP acquisition data to ProtoAcquisitionDataSchema for subject 804434."""

from pathlib import Path

from aind_physiology_fip.data_mappers import ProtoAcquisitionMapper

data_path = "/allen/aind/scratch/vr-foraging/data/804434/804434_2025-11-05T014006Z"
# Write locally first for testing
output_path = Path("fip_804434_2025-11-05T014006Z.json")

print(f"Mapping FIP data from: {data_path}")
acquisition_mapped = ProtoAcquisitionMapper(data_path).map()

print(f"Writing output to: {output_path}")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(acquisition_mapped.model_dump_json(indent=2))

print("✅ Successfully created fip.json")
