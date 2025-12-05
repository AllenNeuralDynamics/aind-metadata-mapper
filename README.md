# aind-metadata-mapper

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-86%25-yellow?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.11-blue?logo=python)

Repository to contain code that will parse source files into aind-data-schema models.

## Usage

The `GatherMetadataJob` is used to create the `data_description.json` and pull the `subject.json` and `procedures.json` from `aind-metadata-service`. Users are expected to provide the `instrument.json` and the `acquisition.json` as well as optional `processing.json`, `quality_control.json` and `model.json`. The job will attempt to validate all of the metadata files, displaying errors, and then will save all metadata fields into the selected folder. Users can then initiate a call to the `aind-data-transfer-service`.

### Using the GatherMetadataJob

Install the metadata mapper (requires Python >=3.11).

Before running the mapper, generate valid `instrument.json` and `acquisition.json` files, plus any optional metadata.

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

Each MapperJob class should inherit from the base MapperJob in `base.py`. The only parameter should be the JobSettings from `base.py`. You cannot add additional parameters to your job or it will not be possible for it to be run automatically on the data-transfer-service.

#### [todo]
