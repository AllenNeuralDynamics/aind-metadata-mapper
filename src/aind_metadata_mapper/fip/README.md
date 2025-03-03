# Fiber Photometry Session Metadata Generator

This module generates standardized session metadata for fiber photometry experiments using a simple ETL (Extract, Transform, Load) pattern.

## Overview
- `job_settings.py`: Defines the required input settings via `JobSettings` class
- `session.py`: Contains the ETL logic to generate a valid session.json file

## Usage

### Example Usage (from tests)
```python
from datetime import datetime
import zoneinfo
from aind_metadata_mapper.fip.session import FIBEtl
from aind_metadata_mapper.fip.job_settings import JobSettings

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

### Actual Usage (from JSON)
```python
import json
from pathlib import Path
from aind_metadata_mapper.fip.session import FIBEtl
from aind_metadata_mapper.fip.job_settings import JobSettings

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
python -m aind_metadata_mapper.fip.session job_settings.json
```