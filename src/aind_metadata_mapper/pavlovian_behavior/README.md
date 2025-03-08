# Pavlovian Behavior Session Metadata Generator

This module generates standardized session metadata for Pavlovian behavior experiments using a simple ETL (Extract, Transform, Load) pattern.

## Overview
- `models.py`: Defines the required input settings via `JobSettings` class
- `session.py`: Contains the ETL logic to generate a valid session.json file
- The resulting `session.json` file is either written to the `output_directory` specified in the job settings, or returned as a JSON string in the ETL response

The ETL process takes experiment settings from either a JSON file or `JobSettings` object and produces standardized session metadata that conforms to the AIND data schema. It can also extract data directly from behavior data files.

## Usage

### Example usage with data extraction from files
This example shows how to use the `BehaviorEtl` class to generate session metadata by extracting data from behavior files.

```python
from pathlib import Path
from aind_metadata_mapper.pavlovian_behavior.session import BehaviorEtl
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings

# Create settings with path to data directory
settings = JobSettings(
    experimenter_full_name=["Test User"],
    subject_id="000000",
    rig_id="behavior_rig_01",
    task_version="1.0.0",
    data_directory="/path/to/behavior/data",
    iacuc_protocol="2115",
)

# Generate session metadata - data will be extracted from files
etl = BehaviorEtl(settings)
response = etl.run_job()
```

### Example usage from JSON file
This example shows how to use the `BehaviorEtl` class to generate session metadata when the job settings are stored in a JSON file.

```python
import json
from pathlib import Path
from aind_metadata_mapper.pavlovian_behavior.session import BehaviorEtl
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings

# Load settings from JSON file
settings_path = Path("job_settings.json")
with open(settings_path, "r") as f:
    settings_data = json.load(f)

# Create JobSettings instance
job_settings = JobSettings(**settings_data)
# Or pass JSON string directly
# etl = BehaviorEtl(job_settings=json.dumps(settings_data))

# Generate session metadata
etl = BehaviorEtl(job_settings)
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
from aind_metadata_mapper.pavlovian_behavior.session import BehaviorEtl
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings

# Create example settings
session_time = datetime(2023, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC"))
settings = JobSettings(
    experimenter_full_name=["Test User"],
    session_start_time=session_time,
    subject_id="000000",
    rig_id="behavior_rig_01",
    task_version="1.0.0",
    stimulus_frame_rate=60.0,
    stimulus_epochs=[{
        "stimulus_name": "Pavlovian",
        "stimulus_start_time": session_time,
        "trials_finished": 100,
        "trials_total": 100,
        "trials_rewarded": 80,
        "reward_consumed_during_epoch": 160
    }],
    notes="Test session",
    iacuc_protocol="2115",
)

# Generate session metadata
etl = BehaviorEtl(settings)
response = etl.run_job()
```

### Example job_settings.json
Here's an example of a job settings JSON file:

```json
{
  "experimenter_full_name": ["Test User"],
  "subject_id": "000000",
  "rig_id": "behavior_rig_01",
  "task_version": "1.0.0",
  "data_directory": "/path/to/behavior/data",
  "iacuc_protocol": "2115"
}
```

## Job Settings Structure
The `JobSettings` class expects:
- `experimenter_full_name`: List of experimenter names
- `session_start_time`: UTC datetime of session start (optional if `data_directory` is provided)
- `session_end_time`: UTC datetime of session end (optional)
- `subject_id`: Subject identifier
- `rig_id`: Identifier for the experimental rig
- `data_directory`: Path to directory containing behavior files (optional, but required if `session_start_time` is not provided)
- `task_name`: Name of the behavioral task (defaults to "Pavlovian")
- `task_version`: Version of the task
- `stimulus_frame_rate`: Frame rate of stimulus presentation in Hz (defaults to 60.0)
- `stimulus_epochs`: List of stimulus epoch configurations (optional if `data_directory` is provided)
  - `stimulus_name`: Name of the stimulus
  - `stimulus_start_time`: Start time of the stimulus epoch
  - `stimulus_end_time`: End time of the stimulus epoch (optional)
  - `trials_finished`: Number of trials completed
  - `trials_total`: Total number of trials
  - `trials_rewarded`: Number of rewarded trials
  - `reward_consumed_during_epoch`: Amount of reward consumed
- `notes`: Additional session notes
- `iacuc_protocol`: Protocol identifier

## Data Extraction
When a `data_directory` is provided, the ETL process will:
1. Look for behavior files matching the pattern `TS_CS1_*.csv` in the `behavior` subdirectory
2. Extract the session start time from the filename
3. Look for trial files matching the pattern `TrialN_TrialType_ITI_*.csv`
4. Extract trial information to populate stimulus epochs
5. Add the original folder name to the notes field

## Command Line Usage
The module can also be run from the command line:

```bash
python -m aind_metadata_mapper.pavlovian_behavior.session job_settings.json
```

## Extending the ETL Process to Include Service Integrations
The ETL implementation could be modified to extend the metadata generation process, particularly for incorporating data from external services. For example, we might want to add optional session metadata fields by querying another service using the subject_id.

To add service integrations, add the service calls to the `_transform` method in `session.py` before the Session object is created. Any data returned from these services must correspond to optional fields defined in the Session schema.