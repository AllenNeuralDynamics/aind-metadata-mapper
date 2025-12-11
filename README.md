# aind-metadata-mapper

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-86%25-yellow?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

Repository to contain code that will parse source files into aind-data-schema models.

## Usage

The `GatherMetadataJob` is used to create the `data_description.json` and pull the `subject.json` and `procedures.json` from `aind-metadata-service`. Users are expected to provide the `instrument.json` and the `acquisition.json` as well as optional `processing.json`, `quality_control.json` and `model.json`. The job will attempt to validate all of the metadata files, displaying errors, and then will save all metadata fields into the selected folder.

### Using the GatherMetadataJob

The following are the minimum **required** settings:

- **`output_dir`** (str): Location where metadata files will be saved. If a `metadata_dir` is not provided, this will also be the location that the job searches for metadata files.
- **`data_description_settings`**:
  - **`project_name`** (str): Project name used to fetch funding and investigator information.
  - **`modalities`** (List[Modality]): List of data modalities for this dataset.

```python
from aind_data_schema_models.modalities import Modality
from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings, DataDescriptionSettings

job_settings = JobSettings(
  output_dir="/path/to/output",
  subject_id="123456",
  data_description_settings=DataDescriptionSettings(
    project_name="<project-name>",
    modalities=[Modality.ECEPHYS],
  )
)

job = GatherMetadataJob(job_settings=job_settings)
job.run_job()
```

#### Default behavior

The GatherMetadataJob attempts to find all of the core metadata files (instrument.json, acquisition.json, etc) and then validates them as a full `Metadata` object.

The job will always prioritize an exact match for a core file when it finds one in the `metadata_dir`.

If no exact match exists, it will construct, fetch, merge or run mappers to generate the appropriate metadata, if it is available.

| File | Method 1 | Method 2 | Method 3 |
|------|----------|----------|----------|
| data_description.json | Exact match in input directory | Construct from settings / fetch from metadata-service |  |
| subject.json | Exact match in input directory | Fetch from metadata-service (requires subject_id) |  |
| procedures.json | Exact match in input directory | Fetch from metadata-service (requires subject_id) |  |
| acquisition.json | Exact match in input directory | Run mappers on `<mapper>.json` files (and merge) | Merge all `acquisition*.json` files |
| instrument.json | Exact match in input directory | Fetch from metadata-service (requires instrument_id) | Merge all `instrument*.json` files |
| processing.json | Exact match in input directory |  |  |
| quality_control.json | Exact match in input directory | Merge all `quality_control*.json` files |  |
| model.json | Exact match in input directory |  |  |

#### Automated mappers

When mappers are developed from the `BaseMapper` class and registered in `mapper_registry.py` they can be automatically run by the GatherMetadataJob. A file matching the mapper name `<mapper>.json` will be turned into a file `acquisition_<mapper>.json` and then merged with any other acquisition files.

#### Optional settings

- **`metadata_dir`** (str, optional): Location of existing metadata files, if different from the `output_dir`. If a file is found here, it will be used directly instead of constructing/fetching it.

- **`subject_id`** (str): Subject ID used to fetch metadata from the service (subject.json, procedures.json). This setting should only be used when an `acquisition.json` is not available.

- **`acquisition_start_time`** (datetime, optional): Acquisition start time in ISO 8601 format. This setting should only be used when an `acquisition.json` is not available.

- **`instrument_settings`**:
  - **`instrument_id`** (str): ID for the instrument used in data collection. When set, the instrument.json will attempt to be fetched from the metadata-service and saved as `instrument_<modality-abbreviation(s)>.json`. If multiple `instrument*.json` files exist after fetching they will be merged.

- **`data_description_settings`**: See [DataDescription](https://aind-data-schema.readthedocs.io/en/latest/data_description.html#datadescription) for details.
  - **`tags`** (list[str], optional)
  - **`group`** (str, optional)
  - **`restrictions`** (str, optional)
  - **`data_summary`** (str, optional)


```python
from datetime import datetime
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.data_name_patterns import Group
from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings, DataDescriptionSettings, InstrumentSettings

job_settings = JobSettings(
    metadata_dir="/path/to/input/",
    output_dir="/path/to/output",
    subject_id="828422",
    acquisition_start_time=datetime.fromisoformat("2025-11-13T17:38:37.079861+00:00"),
    data_description_settings=DataDescriptionSettings(
        project_name="Cognitive flexibility in patch foraging",
        modalities=[Modality.BEHAVIOR, Modality.BEHAVIOR_VIDEOS, Modality.FIB],
        tags=["foraging"],
        group=Group.BEHAVIOR,
        restrictions="Internal use only",
        data_summary="VR foraging task with fiber photometry recording",
    ),
    instrument_settings=InstrumentSettings(
        instrument_id="13A",
    ),
    raise_if_invalid=True,
    raise_if_mapper_errors=True,
    metadata_service_url="http://aind-metadata-service",
)

job = GatherMetadataJob(job_settings=job_settings)
job.run_job()
```

#### Validation settings

- **`raise_if_invalid`** (bool, default=False): Controls validation behavior:
  - `True`: Raises an exception if any fetched metadata is invalid.
  - `False`: Logs a warning or error and continues when validation errors occur.

- **`raise_if_mapper_errors`** (bool, default=True): Controls mapper execution behavior:
  - `True`: Raises an error if any automated mapper (e.g., for instrument-specific formats) fails.
  - `False`: Logs a warning and continues without that mapper's output.

#### Metadata service settings

You probably shouldn't be modifying these.

- **`metadata_service_url`** (str, default=`http://aind-metadata-service`): Base URL of the metadata service.

- **`metadata_service_*_endpoint`** (str): API endpoints for specific metadata types:
  - `metadata_service_subject_endpoint` (default="/api/v2/subject/")
  - `metadata_service_procedures_endpoint` (default="/api/v2/procedures/")
  - `metadata_service_instrument_endpoint` (default="/api/v2/instrument/")

### Developing Mappers

Each MapperJob class should inherit from `BaseMapper` in `base.py`. The only parameter should be the `MapperJobSettings` from `base.py`. You cannot add additional parameters to your job or it will not be possible for it to be run automatically on the data-transfer-service. GatherMetadataJob will then run your mappers automatically when it detects the extracted metadata output.

#### Writing the output file

In your `run_job()` function the final step should be to use the `write_standard_file()` function and pass it the parameters from the job settings. This ensures that any changes we make to how writing files happens in the future will be preserved in your mapper.

```
acquisition.write_standard_file(output_directory=job_settings.output_directory, filename_suffix=filename_suffix)
```

#### Individual mappers

[FIP](src/aind_metadata_mapper/fip/README.md)
