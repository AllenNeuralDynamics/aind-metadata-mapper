# Fiber Photometry Session Metadata Generator

This module generates standardized session metadata for fiber photometry experiments using a simple ETL (Extract, Transform, Load) pattern.

## Overview
- `models.py`: Defines the required input settings via `JobSettings` class
- `session.py`: Contains the ETL logic to generate a valid session.json file
- `utils.py`: Contains utility functions for handling timestamps and file operations
- `example_create_session.py`: Provides a simplified interface for creating session metadata

The ETL process takes experiment settings and produces standardized session metadata that conforms to the AIND data schema.

## Usage

### Simplified Usage with Example Script
The easiest way to generate session metadata is using the example script:

```python
from pathlib import Path
from aind_metadata_mapper.fip.example_create_session import create_metadata

create_metadata(
    subject_id="000000",
    data_directory=Path("/path/to/data"),
    # Optional parameters with defaults:
    output_directory=None,  # defaults to data_directory if not specified
    output_filename="session_fip.json",  # default filename
    experimenter_full_name=["test_experimenter_1", "test_experimenter_2"],
    rig_id="428_9_A_20240617",
    task_version="1.0.0",
    iacuc_protocol="2115",
    mouse_platform_name="mouse_tube_foraging",
    active_mouse_platform=False,
    session_type="Foraging_Photometry",
    task_name="Fiber Photometry",
    notes="Example configuration for fiber photometry rig"
)
```

Or from the command line:
```bash
python -m aind_metadata_mapper.fip.example_create_session \
    --subject-id 000000 \
    --data-directory /path/to/data \
    --output-directory /path/to/output  # optional, defaults to data directory
    --output-filename session_fip.json  # optional, this is the default
```

### Direct ETL Usage
For more control over the metadata generation, you can use the FIBEtl class directly:

```python
from aind_metadata_mapper.fip.session import FIBEtl
from aind_metadata_mapper.fip.models import JobSettings

# Create settings with required fields
settings = JobSettings(
    subject_id="000000",
    data_directory="/path/to/data",
    # output_directory and output_filename are optional:
    output_directory="/path/to/output",  # defaults to data_directory if not specified
    output_filename="session_fip.json",  # this is the default
    experimenter_full_name=["Test User"],
    rig_id="fiber_rig_01",
    mouse_platform_name="mouse_tube_foraging",
    active_mouse_platform=False,
    data_streams=[{
        "detectors": [{
            "exposure_time": "5230.42765",
            "exposure_time_unit": "millisecond",
            "name": "Green CMOS",
            "trigger_type": "Internal",
        }],
        "fiber_connections": [{
            "fiber_name": "Fiber 0",
            "output_power_unit": "microwatt",
            "patch_cord_name": "Patch Cord A",
            "patch_cord_output_power": "20",
        }],
        "light_sources": [{
            "device_type": "Light emitting diode",
            "excitation_power": None,
            "excitation_power_unit": "milliwatt",
            "name": "470nm LED",
        }]
    }],
    notes="Test session",
    iacuc_protocol="2115",
)

# Generate session metadata
etl = FIBEtl(settings)
response = etl.run_job()
```

## Automatic Time Extraction
The ETL process will automatically extract session start and end times from the data files if they are not explicitly provided. It looks for:
- Session start time from filenames matching pattern: `FIP_DataG_YYYY-MM-DDThh_mm_ss.csv`
- Session end time from the timestamps in the CSV data files

## Job Settings Structure
The `JobSettings` class requires:
- `subject_id`: Subject identifier
- `data_directory`: Path to the data files
- `experimenter_full_name`: List of experimenter names
- `rig_id`: Identifier for the experimental rig
- `mouse_platform_name`: Name of the mouse platform used
- `active_mouse_platform`: Whether the platform was active
- `data_streams`: List of stream configurations including:
  - Light sources (LEDs)
  - Detectors (cameras)
  - Fiber connections
- `notes`: Additional session notes
- `iacuc_protocol`: Protocol identifier

Optional settings with defaults:
- `output_directory`: Where to save the session.json file (defaults to data_directory)
- `output_filename`: Name of the output file (defaults to "session_fip.json")

Session start and end times will be automatically extracted from the data files if not provided.