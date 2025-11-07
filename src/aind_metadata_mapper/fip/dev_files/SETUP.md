# FIP Workflow Setup

## Setup Environment

```bash
conda create -n aind-metadata-mapper-dev python=3.11 -y
conda activate aind-metadata-mapper-dev
pip install -e git+https://github.com/AllenNeuralDynamics/aind-metadata-mapper.git@f5224e5a31ad5bda6fa1ab27ef32cd47fed508be#egg=aind-metadata-mapper
pip install -e git+https://github.com/AllenNeuralDynamics/aind-metadata-extractor.git@2cb1258bf86f0fc07b472473b7fd927f1688ab7a#egg=aind-metadata-extractor
pip install -e git+https://github.com/AllenNeuralDynamics/Aind.Physiology.Fip.git@f1d49868697a9fa77c8480a5d44b5c33e3ac65a9#egg=aind-physiology-fip[data]
```

## Run Workflow

**Note:** Run these commands from the root directory of the `aind-metadata-mapper` repository.

### Step 1: Generate ProtoAcquisitionDataSchema JSON

Replace `/path/to/your/fip/data` with your actual FIP acquisition data directory path:

```python
from pathlib import Path
from aind_physiology_fip.data_mappers import ProtoAcquisitionMapper

# Replace with your actual data path
data_path = '/path/to/your/fip/data'
if not Path(data_path).exists():
    raise FileNotFoundError(f"Data path does not exist: {data_path}")

Path('fip_804434.json').write_text(
    ProtoAcquisitionMapper(data_path).map().model_dump_json(indent=2)
)
```

**Note:** If you already have a ProtoAcquisitionDataSchema JSON file, skip to Step 2.

### Step 2: Transform to AIND Data Schema

```bash
python examples/example_fip_mapper.py fip_804434.json acquisition_804434.json
```
