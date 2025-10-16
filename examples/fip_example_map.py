"""Example script demonstrating FIP mapper usage.

This script shows the complete workflow:
1. Extract metadata from raw FIP data files
2. Transform to schema-compliant Acquisition metadata
3. Save as acquisition.json

To run this example:
    conda create -n fip-mapper python=3.11 -y
    conda activate fip-mapper
    pip install aind-metadata-extractor[fip] aind-metadata-mapper
    python examples/fip_example_map.py
"""

from pathlib import Path
from aind_metadata_extractor.fip.extractor import FiberPhotometryExtractor
from aind_metadata_extractor.fip.job_settings import JobSettings
from aind_metadata_mapper.fip.mapper import FIPMapper


DATA_PATH = Path("/allen/aind/scratch/bruno.cruz/fip_tests/781896_2025-07-18T192910Z/fib/fip_2025-07-18T192959Z")
EXAMPLES_DIR = Path(__file__).parent


print("Step 1: Extracting metadata from FIP data files...")
extractor_job_settings = JobSettings(
    data_directory=DATA_PATH,
    mouse_platform_name="wheel",
    local_timezone="America/Los_Angeles",
    output_directory=DATA_PATH,
)
extractor = FiberPhotometryExtractor(job_settings=extractor_job_settings)
intermediate_model = extractor.extract()
print("  ✓ Extracted metadata from FIP data files")


print("\nStep 2: Transforming to schema-compliant Acquisition...")
mapper = FIPMapper(output_filename="fip_example_acquisition.json")
acquisition = mapper.transform(intermediate_model)
print("  ✓ Created Acquisition object")


print("\nStep 3: Saving fip_example_acquisition.json...")
output_file = mapper.write(model=acquisition, output_directory=str(EXAMPLES_DIR))
print(f"  ✓ Saved to {output_file}")

print("\n✨ Complete! FIP metadata successfully mapped to Acquisition schema.")


# Alternative: Use run_job() for a one-step transform + write
# mapper = FIPMapper(output_filename="fip_example_acquisition.json")
# output_file = mapper.run_job(intermediate_model, output_directory=str(EXAMPLES_DIR))

