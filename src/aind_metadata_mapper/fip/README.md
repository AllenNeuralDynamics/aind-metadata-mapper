# FIP (Fiber Photometry) Mapper

This module transforms intermediate FIP metadata (extracted from raw data files) into schema-compliant `Acquisition` metadata.

## Overview

The FIP mapper follows a two-step pipeline:
1. **Extract** - Use `aind-metadata-extractor` to extract metadata from raw FIP data files
2. **Map** - Use this mapper to transform the intermediate model into an `Acquisition` object

## Installation

```bash
pip install aind-metadata-mapper aind-metadata-extractor
```

For development (editable installs):
```bash
pip install -e /path/to/aind-metadata-extractor
pip install -e /path/to/aind-metadata-mapper
```

## Usage

```python
from aind_metadata_extractor.fip.extractor import FiberPhotometryExtractor
from aind_metadata_extractor.fip.job_settings import JobSettings
from aind_metadata_mapper.fip.mapper import FIPMapper

# Step 1: Extract intermediate metadata
job_settings = JobSettings(
    data_directory="/path/to/fip/data",
    mouse_platform_name="wheel",
    local_timezone="America/Los_Angeles",
)
extractor = FiberPhotometryExtractor(job_settings=job_settings)
intermediate_model = extractor.extract()

# Step 2: Map to Acquisition schema
mapper = FIPMapper()
acquisition = mapper.transform(intermediate_model)

# Step 3: Save
mapper.write(acquisition, filename="acquisition.json", output_directory="/output/path")
```

## What Gets Mapped

### From Intermediate Model → Acquisition

- `subject_id` → `subject_id`
- `session_start_time` → `acquisition_start_time`
- `session_end_time` → `acquisition_end_time`
- `experimenter_full_name` → `experimenters`
- `rig_id` → `instrument_id`
- `session_type` → `acquisition_type`
- `iacuc_protocol` → `protocol_id[0]`
- `notes` → `notes`

### Subject Details (nested)

- `mouse_platform_name` → `subject_details.mouse_platform_name`
- `animal_weight_prior` → `subject_details.animal_weight_prior`
- `animal_weight_post` → `subject_details.animal_weight_post`
- `anaesthesia` → `subject_details.anaesthesia`

### Hardware → DataStream

From `rig_config`:
- Light sources (`light_source_*`) → `LightEmittingDiodeConfig` objects
- Cameras (`camera_*`) → `DetectorConfig` objects
- Controller (`cuttlefish_fip`) → Added to `active_devices`
- ROI settings (`roi_settings`) → `PatchCordConfig` with `Channel` objects for each ROI

**Note:** The FIB modality (fiber photometry abbreviation) requires either `PatchCordConfig` or `FiberAssemblyConfig` to be present in configurations to represent the fiber optic connections.

## Example

See `examples/fip_example_map.py` for a complete working example.

## Requirements

- `aind-data-schema>=2.0.5`
- `aind-metadata-extractor` (with FIP support)

