# Implementing Behavior ETL Module

This guide outlines how to implement a Behavior ETL module following the same pattern as the FIP (Fiber Photometry) module.

## File Structure
Create the following files:
src/aind_metadata_mapper/behavior/
├── __init__.py
├── models.py
├── session.py
└── README.md

## 1. models.py
Define a `JobSettings` class that inherits from `BaseJobSettings`:

from datetime import datetime
from typing import List, Optional, Literal

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Settings for generating Behavior session metadata."""

    job_settings_name: Literal["Behavior"] = "Behavior"

    # Required fields
    experimenter_full_name: List[str]
    session_start_time: datetime
    session_end_time: Optional[datetime] = None
    subject_id: str
    rig_id: str
    
    # Behavior-specific fields
    task_name: str

    stimulus_frame_rate: float
    response_window: List[float]  # [start_time, end_time] in seconds
    
    # Optional fields with defaults
    session_type: str = "Behavior"
    iacuc_protocol: str
    notes: str
    protocol_id: List[str] = []
    
    @classmethod
    def from_args(cls, args: List[str]):
        """Create JobSettings from command line arguments."""
        # Implementation for CLI usage
        pass

## 2. session.py
Implement the ETL process:

"""Module for creating Behavior session metadata.

This module demonstrates a simple ETL pattern for creating session metadata,
with hooks for future extension to fetch additional data from external
services.
"""

import sys
import json
from typing import Union

from aind_data_schema.core.session import Session
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.behavior.models import JobSettings


class BehaviorEtl(GenericEtl[JobSettings]):
    """Creates behavior session metadata with ETL pattern."""

    def __init__(self, job_settings: Union[str, JobSettings]):
        """Initialize ETL with job settings.

        Args:
            job_settings: Either a JobSettings object or a JSON string that can
                be parsed into one
        """
        if isinstance(job_settings, str):
            job_settings = JobSettings(**json.loads(job_settings))
        super().__init__(job_settings)

    def _extract(self) -> JobSettings:
        """Extract metadata from job settings and external sources."""
        return self.job_settings

    def _transform(self, settings: JobSettings) -> Session:
        """Transform extracted data into a valid Session object."""
        # Hook for service integrations - gather any additional optional fields
        # Example service calls:
        # - Query LIMS for subject procedure history using settings.subject_id
        # - Get configuration details from rig control system using settings.rig_id
        # - Fetch task parameters from a task database
        #
        # These calls should gather data for optional Session fields defined in the schema

        # Create session with all available data
        session = Session(
            experimenter_full_name=settings.experimenter_full_name,
            session_start_time=settings.session_start_time,
            session_end_time=settings.session_end_time,
            session_type=settings.session_type,
            rig_id=settings.rig_id,
            subject_id=settings.subject_id,
            iacuc_protocol=settings.iacuc_protocol,
            notes=settings.notes,
            # Add behavior-specific fields
            task_name=settings.task_name,
            stimulus_frame_rate=settings.stimulus_frame_rate,
            response_window=settings.response_window,
            # Add any optional fields gathered from services here
        )

        return session

    def run_job(self) -> JobResponse:
        """Run the ETL job and return a JobResponse."""
        extracted = self._extract()
        transformed = self._transform(extracted)
        return self._load(transformed, self.job_settings.output_directory)


if __name__ == "__main__":  # pragma: no cover
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = BehaviorEtl(job_settings=main_job_settings)
    etl.run_job()

## 3. README.md
Create a README with usage examples:

# Behavior Session Metadata Generator

This module generates standardized session metadata for behavior experiments using a simple ETL (Extract, Transform, Load) pattern.

## Overview
- `models.py`: Defines the required input settings via `JobSettings` class
- `session.py`: Contains the ETL logic to generate a valid session.json file
- The resulting `session.json` file is either written to the `output_directory` specified in the job settings, or returned as a JSON string in the ETL response

The ETL process takes experiment settings from either a JSON file or `JobSettings` object and produces standardized session metadata that conforms to the AIND data schema.

## Usage

### Example usage from JSON file
This example shows how to use the `BehaviorEtl` class to generate session metadata when the job settings are stored in a JSON file.

import json
from pathlib import Path
from aind_metadata_mapper.behavior.session import BehaviorEtl
from aind_metadata_mapper.behavior.models import JobSettings

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

### Example usage with JobSettings object
This example shows how to use the `JobSettings` object directly to generate session metadata.

from datetime import datetime
import zoneinfo
from aind_metadata_mapper.behavior.session import BehaviorEtl
from aind_metadata_mapper.behavior.models import JobSettings

# Create example settings
session_time = datetime(2023, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC"))
settings = JobSettings(
    experimenter_full_name=["Test User"],
    session_start_time=session_time,
    subject_id="000000",
    rig_id="behavior_rig_01",
    task_name="VisualDiscrimination",
    stimulus_frame_rate=60.0,
    response_window=[0.15, 1.0],
    notes="Test session",
    iacuc_protocol="2115",
)

# Generate session metadata
etl = BehaviorEtl(settings)
response = etl.run_job()

## Job Settings Structure
The `JobSettings` class expects:
- `experimenter_full_name`: List of experimenter names
- `session_start_time`: UTC datetime of session start
- `session_end_time`: UTC datetime of session end (optional)
- `subject_id`: Subject identifier
- `rig_id`: Identifier for the experimental rig
- `task_name`: Name of the behavioral task
- `stimulus_frame_rate`: Frame rate of stimulus presentation in Hz
- `response_window`: Time window for valid responses [start, end] in seconds
- `notes`: Additional session notes
- `iacuc_protocol`: Protocol identifier

## Command Line Usage
The module can also be run from the command line:

python -m aind_metadata_mapper.behavior.session job_settings.json

## Extending the ETL Process to Include Service Integrations
The ETL implementation could be modified to extend the metadata generation process, particularly for incorporating data from external services. For example, we might want to add optional session metadata fields by querying another service using the subject_id.

To add service integrations, add the service calls to the `_transform` method in `session.py` before the Session object is created. Any data returned from these services must correspond to optional fields defined in the Session schema.

## 4. Tests
Create test files:
tests/test_behavior/
├── __init__.py
├── test_models.py
└── test_session.py

Key test cases to implement:
1. Test JobSettings validation
2. Test ETL initialization from string and object
3. Test the transform method
4. Test the run_job method with mocked file operations
5. Test CLI usage

## Integration
Update `gather_metadata.py` to include the new Behavior ETL in the session metadata gathering process, similar to how FIP is integrated.