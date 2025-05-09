# SmartSPIM Acquisition ETL
This package maps acquisition metadata from smartspim microscope files. 

Installation:
```bash
pip install -e .[smartspim]
```

### Usage
You can directly generate the acquisition from command line:
```bash
python -m aind_metadata_mapper.pavlovian_behavior.example_create_session \
    --subject-id 000000 \
    --metadata_service_path http://service/smartspim/imaging \
    --input_source /path/to/data \
    --output_directory /path/to/output \
```
Or in python:
```python
from aind_metadata_mapper.smartspim.acquisition import SmartspimETL
from aind_metadata_mapper.smartspim.models import JobSettings

# Create settings with required fields
settings = JobSettings(
    subject_id="000000",
    metadata_service_domain="http://service/smartspim/imaging",
    input_source="path/to/data",
    output_directory="path/to/output"
)
etl = SmartspimETL(settings)
etl.run_job() # Writes etl to output dir
    

# Generate session metadata
etl = ETL(settings)
response = etl.run_job()
```
