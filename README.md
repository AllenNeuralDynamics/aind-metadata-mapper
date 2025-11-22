# aind-metadata-mapper

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-86%25-yellow?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

Repository to contain code that will parse source files into aind-data-schema models.

## Usage

The `GatherMetadataJob` is used to create the `data_description.json` and pull the `subject.json` and `procedures.json` from `aind-metadata-service`. Users are expected to provide the `instrument.json` and the `acquisition.json` as well as optional `processing.json`, `quality_control.json` and `model.json`. The job will attempt to validate all of the metadata files, displaying errors, and then will save all metadata fields into the selected folder. Users can then initiate a call to the `aind-data-transfer-service`.

### Using the GatherMetadataJob

Install the metadata mapper (requires Python >=3.10).

```{python}
pip install aind-metadata-mapper
```

#### Default behavior

The GatherMetadataJob attempts to find all of the core metadata files (instrument.json, acquisition.json, etc) and then validates them as a full `Metadata` object.

The job will always prioritize an exact match for the core file. If that fails, it will construct, fetch, merge or run mappers to generate the appropriate metadata, if it is available.

| File | Method 1 | Method 2 | Method 3 |
|------|----------|----------|----------|
| data_description.json | Check user directory | Construct from settings and funding service |  |
| subject.json | Check user directory | Fetch from metadata service (requires subject_id) |  |
| procedures.json | Check user directory | Fetch from metadata service (requires subject_id) |  |
| acquisition.json | Check user directory | Run mappers on `<mapper>.json` files | Merge all `acquisition_*.json` files |
| instrument.json | Check user directory | Merge all `instrument*.json` files | Fetch from metadata service (requires instrument_id) |
| processing.json | Check user directory |  |  |
| quality_control.json | Check user directory | Merge all `quality_control*.json` files |  |
| model.json | Check user directory |  |  |

#### Minimal settings

The following settings are **required**:

- **`output_dir`** (str): Location where all metadata files will be saved.
- **`subject_id`** (str): Subject ID used to fetch metadata from the service (subject.json, procedures.json).
- **`data_description_settings`**:
  - **`project_name`** (str): Project name used to fetch funding and investigator information.
  - **`modalities`** (List[Modality]): List of data modalities for this dataset (e.g., `["ecephys", "behavior"]`).

Example minimal configuration:

```python
{
  "output_dir": "/path/to/output",
  "subject_id": "123456",
  "data_description_settings": {
    "project_name": "<project-name>",
    "modalities": Modality.ECEPHYS,
  }
}
```

#### Automated mappers

When mappers are developed from the `BaseMapper` class and registered in `mapper_registry.py` they can be automatically run by the GatherMetadataJob. A file matching the mapper name `<mapper>.json` will then be turned into the `acquisition.json`. Multiple mappers can be run and the results will be merged.

#### Optional settings

- **`metadata_dir`** (str, optional): Location of existing metadata files. If a file is found here, it will be used directly instead of constructing/fetching it. Supports merging multiple files with prefixes (e.g., `instrument_0.json`, `instrument_1.json` will be merged).

- **`acquisition_start_time`** (datetime, optional): Acquisition start time in ISO 8601 format. **Important edge cases:**
  - If `acquisition.json` exists in `metadata_dir`, that value takes precedence and will override this setting.
  - If `acquisition_start_time` is provided in settings AND found in `acquisition.json`, they must match exactly (configurable behavior below).
  - If neither is provided, `GatherMetadataJob` will raise an error.

- **`instrument_settings`**:
  - **`instrument_id`** (str): Identifier for the instrument used in data collection.

- **`data_description_settings`**: See [DataDescription](https://aind-data-schema.readthedocs.io/en/latest/data_description.html#datadescription) for details.
  - **`tags`** (list[str], optional)
  - **`group`** (str, optional)
  - **`restrictions`** (str, optional)
  - **`data_summary`** (str, optional)

#### Validation settings

- **`raise_if_invalid`** (bool, default=False): Controls validation behavior:
  - `True`: Raises an error if any fetched metadata is invalid.
  - `False`: Logs a warning and continues with best-effort validation.
  - Also applies to `acquisition_start_time` mismatch validation (when both settings and acquisition.json provide a value).

- **`raise_if_mapper_errors`** (bool, default=True): Controls mapper execution behavior:
  - `True`: Raises an error if any automated mapper (e.g., for instrument-specific formats) fails.
  - `False`: Logs a warning and continues without that mapper's output.

#### Metadata service settings

In general, you shouldn't be modifying these.

- **`metadata_service_url`** (str, default=`http://aind-metadata-service`): Base URL of the metadata service.

- **`metadata_service_*_endpoint`** (str): API endpoints for specific metadata types:
  - `metadata_service_subject_endpoint` (default="/api/v2/subject/")
  - `metadata_service_procedures_endpoint` (default="/api/v2/procedures/")
  - `metadata_service_instrument_endpoint` (default="/api/v2/instrument/")

### Developing Mappers

Each MapperJob class should inherit from `BaseMapper` in `base.py`. The only parameter should be the `MapperJobSettings` from `base.py`. You cannot add additional parameters to your job or it will not be possible for it to be run automatically on the data-transfer-service. GatherMetadataJob will then run your mappers automatically when it detects the extracted metadata output.

#### [todo]
