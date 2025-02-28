# Fiber Photometry Session Metadata Generator

This module generates standardized session metadata for fiber photometry experiments.

## Overview
- `models.py`: Defines the required input settings via `JobSettings`
- `session.py`: Contains the ETL logic to generate a valid session.json file

## Usage
```python
from aind_metadata_mapper.fip.session import FIBEtl
from aind_metadata_mapper.fip.models import JobSettings

# Create settings (typically loaded from a json file)
settings = JobSettings(
    experimenter_full_name=["John Doe"],
    session_start_time=datetime.now(timezone.utc),
    subject_id="123456",
    # ... other required fields ...
)

# Generate session metadata
etl = FIBEtl(settings)
response = etl.run_job()
```