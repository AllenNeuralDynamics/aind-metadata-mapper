import json
from pathlib import Path
from aind_metadata_mapper.smartspim.mapper import SmartspimMapper

test_data_path = Path(__file__).parent.parent.parent / "tests" / "resources" / "smartspim" / "smartspim.json"
output_dir = Path(__file__).parent

with open(test_data_path, "r") as f:
    test_metadata = json.load(f)

mapper = SmartspimMapper()
acquisition = mapper.transform(test_metadata)

output_file = output_dir / "acquisition.json"
with open(output_file, "w") as f:
    json.dump(acquisition.model_dump(mode="json"), f, indent=2)

print(f"Integration test completed successfully!")
print(f"Output written to: {output_file}")
print(f"\nAcquisition Summary:")
print(f"  Subject ID: {acquisition.subject_id}")
print(f"  Specimen ID: {acquisition.specimen_id}")
print(f"  Instrument ID: {acquisition.instrument_id}")
print(f"  Acquisition Type: {acquisition.acquisition_type}")
print(f"  Start Time: {acquisition.acquisition_start_time}")
print(f"  End Time: {acquisition.acquisition_end_time}")
print(f"  Data Streams: {len(acquisition.data_streams)}")
