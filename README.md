# aind-metadata-mapper

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-86%25-yellow?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

Repository to contain code that will parse source files into aind-data-schema models.

## Usage

### Using the GatherMetadataJob

Install the metadata mapper (requires Python >=3.10).

```{python}
pip install aind-metadata-mapper
```

```{python}
from aind_metadata_mapper.models import JobSettings
from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_data_schema_models.modalities import Modality

# Create JobSettings with minimal required parameters
settings = JobSettings(
    metadata_dir=".",  # Directory where metadata files are currently stored and will be saved
    subject_id="123456",         # Replace with actual subject ID
    project_name="my_project",  # Replace with actual project name
    modalities=[Modality.ECEPHYS]  # Replace with relevant modalities
)

# Create and run the job
job = GatherMetadataJob(settings=settings)
job.run_job()
```

### Using the individual mappers

[todo]
