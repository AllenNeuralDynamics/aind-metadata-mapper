# FIP (Fiber Photometry) Mapper

Transforms FIP metadata extracted from acquisition systems into AIND Data Schema 2.0 Acquisition format.

## Overview

The mapper validates input JSON against the FIP schema (defined in aind-metadata-extractor) and creates an Acquisition object with the proper structure for fiber photometry experiments. It generates three channels per fiber (Green, Isosbestic, Red) reflecting the temporal multiplexing used in FIP rigs.

Device names and hardware configuration come directly from the rig metadata. The mapper queries the metadata service for intended measurements and implanted fiber information when available, otherwise infers fiber count from ROI settings.

## Installation

```bash
pip install aind-metadata-mapper
```

For development with the FIP schema:
```bash
cd /path/to/aind-metadata-extractor
git checkout feat-add-fip-json-schema-model
pip install -e .

cd /path/to/aind-metadata-mapper
git checkout add-fip-mapper
pip install -e .
```

## Usage

```python
import json
from aind_metadata_mapper.fip.mapper import FIPMapper

# Load extracted metadata (must conform to fip.json schema)
with open("fip_metadata.json") as f:
    metadata = json.load(f)

# Create mapper and transform
mapper = FIPMapper()
acquisition = mapper.transform(metadata)

# Write output
output_path = mapper.write(acquisition, output_directory="/output/path")
```

The mapper expects input JSON with this structure:
- `session` - experiment metadata (subject, experimenters, notes, etc.)
- `rig` - hardware configuration (cameras, LEDs, ROI settings, etc.)
- `data_stream_metadata` - timing information

## What Gets Mapped

### Core Fields
- `session.subject` → `subject_id`
- `session.experiment` → `acquisition_type`
- `session.experimenter` → `experimenters`
- `session.notes` → `notes`
- `rig.rig_name` → `instrument_id`
- `data_stream_metadata[0].start_time` → `acquisition_start_time`
- `data_stream_metadata[0].end_time` → `acquisition_end_time`

### Hardware Devices
- `rig.light_source_*` → LED configurations with wavelengths (415nm, 470nm, 565nm)
- `rig.camera_green_iso` → Green/Isosbestic detector
- `rig.camera_red` → Red detector
- `rig.cuttlefish_fip` → Controller device
- `rig.roi_settings` → Determines fiber count and creates patch cord configurations

### Channels
Each fiber gets three channels:
- Green (470nm excitation, 520nm emission)
- Isosbestic (415nm excitation, 520nm emission)
- Red (565nm excitation, 590nm emission)

Camera exposure times are extracted from `light_source_*.task.delta_1` (in microseconds).

## Example

See `examples/example_fip_mapper.py` for a working example.

## Validation

Input JSON is validated against the FIP schema (`fip.json` in aind-metadata-extractor). The mapper will raise a `ValueError` if validation fails.

## Requirements

- `aind-data-schema>=2.0.0`
- `aind-metadata-extractor` (with FIP schema support)
- `jsonschema`
