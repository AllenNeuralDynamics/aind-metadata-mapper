import json
from pathlib import Path
from aind_metadata_mapper.fiber_photometry.session import ETL
from aind_metadata_mapper.fiber_photometry.models import JobSettings

# Load settings from JSON file
settings_file_name = "job_settings_fiber_photometry.json"
settings_path = Path(__file__).resolve().parent
with open(settings_path / settings_file_name, "r") as f:
    settings_data = json.load(f)

# Create JobSettings instance
job_settings = JobSettings(**settings_data)
# Or pass JSON string directly
# etl = FIBEtl(job_settings=json.dumps(settings_data))

# Generate session metadata
etl = ETL(job_settings)
response = etl.run_job()
print(response.data)