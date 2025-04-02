# Pavlovian Behavior Session Metadata Generator

This module generates standardized session metadata for Pavlovian behavior experiments using a simple ETL (Extract, Transform, Load) pattern.

## Overview
- `models.py`: Defines the required input settings via `JobSettings` class
- `session.py`: Contains the ETL logic to generate a valid session.json file
- `utils.py`: Contains utility functions for handling timestamps and trial data extraction
- `example_create_session.py`: Provides a simplified interface for creating session metadata

The ETL process takes experiment settings and produces standardized session metadata that conforms to the AIND data schema.

## Usage

### Simplified Usage with Example Script
The easiest way to generate session metadata is using the example script:

```python
from pathlib import Path
from aind_metadata_mapper.pavlovian_behavior.example_create_session import create_metadata

create_metadata(
    subject_id="000000",
    data_directory=Path("/path/to/data"),
    output_directory=Path("/path/to/output"),
    output_filename="session_pavlovian.json",
    # Optional parameters with defaults:
    experimenter_full_name=["test_experimenter_1", "test_experimenter_2"],
    rig_id="428_9_0_20240617",
    iacuc_protocol="2115",
    mouse_platform_name="mouse_tube_pavlovian",
    active_mouse_platform=False,
    session_type="Pavlovian_Conditioning",
    notes="Example configuration for Pavlovian behavior"
)
```

Or from the command line:
```bash
python -m aind_metadata_mapper.pavlovian_behavior.example_create_session \
    --subject-id 000000 \
    --data-directory /path/to/data \
    --output-directory /path/to/output \
    --output-filename session_pavlovian.json
```

### Direct ETL Usage
For more control over the metadata generation, you can use the ETL class directly:

```python
from aind_metadata_mapper.pavlovian_behavior.session import ETL
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings

# Create settings with required fields
settings = JobSettings(
    subject_id="000000",
    data_directory="/path/to/data",
    experimenter_full_name=["Test User"],
    rig_id="pav_rig_01",
    iacuc_protocol="2115",
    mouse_platform_name="mouse_tube_pavlovian",
    active_mouse_platform=False,
    data_streams=[{
        "stream_modalities": [Modality.BEHAVIOR],
        "light_sources": [{
            "name": "IR LED",
            "device_type": "Light emitting diode",
            "excitation_power": None,
            "excitation_power_unit": "milliwatt",
        }],
        "software": [{
            "name": "Bonsai",
            "version": "",
            "url": "",
            "parameters": {},
        }]
    }],
    notes="Test session"
)

# Generate session metadata
etl = ETL(settings)
response = etl.run_job()
```

## Automatic Data Extraction
The ETL process will automatically extract session timing and trial information from the behavior files. It looks for:
- Session start time from filenames matching pattern: `TS_CS1_YYYY-MM-DDThh_mm_ss.csv`
- Trial information from files matching: `TrialN_TrialType_ITI_*.csv`
- Session end time calculated from trial ITIs
- Number of trials and rewards from trial data

## File Format Requirements
The ETL process expects specific file formats:
- Behavior files must be named: `TS_CS1_YYYY-MM-DDThh_mm_ss.csv`
  - Example: `TS_CS1_2024-01-01T15_49_53.csv`
- Trial files must be named: `TrialN_TrialType_ITI_*.csv`
  - Example: `TrialN_TrialType_ITI_001.csv`
- Files should be in a `behavior` subdirectory or main data directory
- Trial files must contain columns:
  - `TrialNumber`: Sequential trial numbers
  - `TotalRewards`: Cumulative rewards given
  - `ITI_s`: Inter-trial intervals in seconds

## Job Settings Structure
The `JobSettings` class requires:
- `subject_id`: Subject identifier
- `data_directory`: Path to the behavior files
- `experimenter_full_name`: List of experimenter names
- `rig_id`: Identifier for the experimental rig
- `iacuc_protocol`: Protocol identifier
- `mouse_platform_name`: Name of the mouse platform used
- `active_mouse_platform`: Whether the platform was active
- `data_streams`: List of stream configurations including:
  - Light sources (IR LED)
  - Software (Bonsai)

Optional fields:
- `output_directory`: Where to save the session.json file
- `output_filename`: Name of the output file (default: session_pavlovian.json)
- `notes`: Additional session notes
- `reward_units_per_trial`: Units of reward per successful trial (default: 2.0)

Session timing and trial information will be automatically extracted from the behavior files.

## Extending the ETL Process to Include Service Integrations
The ETL implementation could be modified to extend the metadata generation process, particularly for incorporating data from external services. For example, we might want to add optional session metadata fields by querying another service using the subject_id.

To add service integrations, add the service calls to the `_transform` method in `session.py` before the Session object is created. Any data returned from these services must correspond to optional fields defined in the Session schema.