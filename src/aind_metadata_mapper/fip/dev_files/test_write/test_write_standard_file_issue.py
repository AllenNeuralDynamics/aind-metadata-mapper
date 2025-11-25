#!/usr/bin/env python3
"""Demonstrates write_standard_file extension issue with suffixes."""

from datetime import datetime, timezone
from pathlib import Path

from aind_data_schema.core.acquisition import Acquisition

# Create minimal acquisition
now = datetime.now(timezone.utc).isoformat()
acquisition = Acquisition.model_validate(
    {
        "subject_id": "test",
        "acquisition_start_time": now,
        "acquisition_end_time": now,
        "instrument_id": "test",
        "acquisition_type": "test",
        "data_streams": [],
    }
)

output_dir = Path(__file__).parent
print(f"Writing to: {output_dir}\n")

# Without suffix: creates acquisition.json ✓
acquisition.write_standard_file(output_directory=output_dir, suffix=None)
file_no_suffix = list(output_dir.glob("acquisition*"))[0]
print("Without suffix:")
print(f"  File: {file_no_suffix}")
print(f"  Name: {file_no_suffix.name}")
print(f"  Has .json extension: {file_no_suffix.suffix == '.json'}\n")

# With suffix: creates acquisition_fip (no .json) ✗
acquisition.write_standard_file(output_directory=output_dir, suffix="_fip")
file_with_suffix = list(output_dir.glob("acquisition_fip*"))[0]
print("With suffix '_fip':")
print(f"  File: {file_with_suffix}")
print(f"  Name: {file_with_suffix.name}")
print(f"  Has .json extension: {file_with_suffix.suffix == '.json'}")
print("\nExpected: acquisition_fip.json")
print(f"Actual:   {file_with_suffix.name}")
