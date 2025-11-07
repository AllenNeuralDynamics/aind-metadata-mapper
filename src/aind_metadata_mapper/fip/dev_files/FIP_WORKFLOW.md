# FIP Metadata Mapping Workflow

Complete instructions for replicating the FIP metadata mapping workflow using ProtoAcquisitionMapper and FIPMapper.

## Prerequisites

- Python 3.11
- Conda (or equivalent Python environment manager)
- Access to Allen Institute network storage (`/allen/aind/scratch/...`)

## Step 1: Create and Activate Conda Environment

```bash
conda create -n aind-metadata-mapper-dev python=3.11 -y
conda activate aind-metadata-mapper-dev
```

## Step 2: Install Dependencies

Install all three packages in editable mode using commit hashes to ensure exact versions:

```bash
# Install aind-metadata-mapper (add-fip-mapper branch)
pip install -e git+https://github.com/AllenNeuralDynamics/aind-metadata-mapper.git@f5224e5a31ad5bda6fa1ab27ef32cd47fed508be#egg=aind-metadata-mapper

# Install aind-metadata-extractor (feat-add-fip-json-schema-model branch)
pip install -e git+https://github.com/AllenNeuralDynamics/aind-metadata-extractor.git@2cb1258bf86f0fc07b472473b7fd927f1688ab7a#egg=aind-metadata-extractor

# Install Aind.Physiology.Fip (main branch) with data extras
pip install -e git+https://github.com/AllenNeuralDynamics/Aind.Physiology.Fip.git@f1d49868697a9fa77c8480a5d44b5c33e3ac65a9#egg=aind-physiology-fip[data]
```

**Note:** Installing from git URLs with commit hashes will clone the repos into `src/` directories. For editable development, you may prefer to clone manually:

```bash
# Alternative: Clone repositories manually
git clone https://github.com/AllenNeuralDynamics/aind-metadata-mapper.git
cd aind-metadata-mapper
git checkout f5224e5a31ad5bda6fa1ab27ef32cd47fed508be
pip install -e .

cd ..
git clone https://github.com/AllenNeuralDynamics/aind-metadata-extractor.git
cd aind-metadata-extractor
git checkout 2cb1258bf86f0fc07b472473b7fd927f1688ab7a
pip install -e .

cd ..
git clone https://github.com/AllenNeuralDynamics/Aind.Physiology.Fip.git
cd Aind.Physiology.Fip
git checkout f1d49868697a9fa77c8480a5d44b5c33e3ac65a9
pip install -e .[data]

cd ..
```

## Step 3: Generate ProtoAcquisitionDataSchema JSON

Create a Python script to map raw FIP data to ProtoAcquisitionDataSchema format:

```python
# map_fip_data.py
from pathlib import Path
from aind_physiology_fip.data_mappers import ProtoAcquisitionMapper

# Path to the FIP acquisition data directory
data_path = "/allen/aind/scratch/vr-foraging/data/804434/804434_2025-11-05T014006Z"

# Output path (adjust as needed)
output_path = Path("fip_804434_2025-11-05T014006Z.json")

print(f"Mapping FIP data from: {data_path}")
acquisition_mapped = ProtoAcquisitionMapper(data_path).map()

print(f"Writing output to: {output_path}")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(acquisition_mapped.model_dump_json(indent=2))

print("✅ Successfully created ProtoAcquisitionDataSchema JSON")
```

Run it:

```bash
python map_fip_data.py
```

This creates `fip_804434_2025-11-05T014006Z.json` containing the ProtoAcquisitionDataSchema.

## Step 4: Transform to AIND Data Schema 2.0 Acquisition

Use the example mapper to transform the ProtoAcquisitionDataSchema JSON to the final Acquisition format:

```bash
cd aind-metadata-mapper
python examples/example_fip_mapper.py fip_804434_2025-11-05T014006Z.json acquisition_804434.json
```

This will:
1. Load and validate the ProtoAcquisitionDataSchema JSON
2. Transform it to AIND Data Schema 2.0 Acquisition format
3. Write the output to `examples/acquisition_804434.json`

## Expected Output

The example script will display:
- Subject ID: 804434
- Instrument ID: 13A
- Acquisition type: FIP
- Start/end times
- Experimenters
- Data stream information
- Device configurations

## Troubleshooting

### Missing `contraqctor` dependency
If you see `ModuleNotFoundError: No module named 'contraqctor'`, ensure you installed the FIP package with `[data]` extras:
```bash
pip install -e .[data]
```

### Protocol file not found warning
The mapper looks for `protocols.yaml` in the project root. This is optional and the mapper will still work without it.

### Metadata service warnings
Warnings about fetching procedures or intended measurements are expected if the metadata service is unavailable. The mapper will use defaults.

## Repository Commits Used

- **aind-metadata-mapper**: `f5224e5a31ad5bda6fa1ab27ef32cd47fed508be` (branch: `add-fip-mapper`)
- **aind-metadata-extractor**: `2cb1258bf86f0fc07b472473b7fd927f1688ab7a` (branch: `feat-add-fip-json-schema-model`)
- **Aind.Physiology.Fip**: `f1d49868697a9fa77c8480a5d44b5c33e3ac65a9` (branch: `main`)
