# Fiber Photometry Session Metadata Generator

This module generates standardized session metadata for fiber photometry experiments using a simple ETL (Extract, Transform, Load) pattern.

## Overview
- `models.py`: Defines the required input settings via `JobSettings` class
- `session.py`: Contains the ETL logic to generate a valid session.json file
- The resulting `session.json` file is either written to the `output_directory` specified in the job settings, or returned as a JSON string in the ETL response

The ETL process takes experiment settings from either a JSON file or `JobSettings` object and produces standardized session metadata that conforms to the AIND data schema.

## Usage

### Example usage from JSON file
This example shows how to use the `FIBEtl` class to generate session metadata when the job settings are stored in a JSON file.
```python
import json
from pathlib import Path
from aind_metadata_mapper.fiber_photometry.session import ETL
from aind_metadata_mapper.fiber_photometry.models import JobSettings

# Load settings from JSON file
settings_path = Path("job_settings.json")
with open(settings_path, "r") as f:
    settings_data = json.load(f)

# Create JobSettings instance
job_settings = JobSettings(**settings_data)
# Or pass JSON string directly
# etl = FIBEtl(job_settings=json.dumps(settings_data))

# Generate session metadata
etl = FIBEtl(job_settings)
response = etl.run_job()

# If output_directory was specified in settings, the session file will be written there
# Otherwise, access the session JSON string from the response
session_json = response.data
```

### Example usage with JobSettings object
This example shows how to use the `JobSettings` object directly to generate session metadata.
```python
from datetime import datetime
import zoneinfo
from aind_metadata_mapper.fiber_photometry.session import FIBEtl
from aind_metadata_mapper.fiber_photometry.models import JobSettings

# Create example settings
session_time = datetime(1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC"))
settings = JobSettings(
    experimenter_full_name=["Test User"],
    session_start_time=session_time,
    subject_id="000000",
    rig_id="fiber_rig_01",
    mouse_platform_name="Disc",
    active_mouse_platform=False,
    data_streams=[{
        "stream_start_time": session_time,
        "stream_end_time": session_time,
        "light_sources": [{
            "name": "470nm LED",
            "excitation_power": 0.020,
            "excitation_power_unit": "milliwatt",
        }],
        "detectors": [{
            "name": "Hamamatsu Camera",
            "exposure_time": 10,
            "trigger_type": "Internal",
        }],
        "fiber_connections": [{
            "patch_cord_name": "Patch Cord A",
            "patch_cord_output_power": 40,
            "output_power_unit": "microwatt",
            "fiber_name": "Fiber A",
        }]
    }],
    notes="Test session",
    iacuc_protocol="2115",
)

# Generate session metadata
etl = FIBEtl(settings)
response = etl.run_job()
```

## Job Settings Structure
The `JobSettings` class expects:
- `experimenter_full_name`: List of experimenter names
- `session_start_time`: UTC datetime of session start
- `session_end_time`: UTC datetime of session end (optional)
- `subject_id`: Subject identifier
- `rig_id`: Identifier for the experimental rig
- `mouse_platform_name`: Name of the mouse platform used
- `active_mouse_platform`: Whether the platform was active
- `data_streams`: List of stream configurations including:
  - Light sources (LEDs)
  - Detectors (cameras)
  - Fiber connections
- `notes`: Additional session notes
- `iacuc_protocol`: Protocol identifier

## Command Line Usage
The module can also be run from the command line:
```bash
python -m aind_metadata_mapper.fiber_photometry.session job_settings.json
```

## Extending the ETL Process to Include Service Integrations
The ETL implementation could be modified to extend the metadata generation process, particularly for incorporating data from external services. For example, we might want to add optional session metadata fields by querying another service using the subject_id.

To add service integrations, add the service calls to the `_transform` method in `session.py` before the Session object is created. Any data returned from these services must correspond to optional fields defined in the Session schema.