# Configuration Examples

This directory contains example configuration files for the Pavlovian behavior and fiber photometry ETL processes.

## File Naming Conventions

Configuration files follow a consistent naming pattern:
- `{modality}_session_params.json`: Contains session parameters for a specific modality
- `{modality}_data_streams.json`: Contains data stream configurations for a specific modality

Where `{modality}` is one of:
- `pavlovian_behavior`: For Pavlovian behavior experiments
- `fiber_photometry`: For fiber photometry experiments

## Configuration Files

### Session Parameter Files
- `pavlovian_behavior_session_params.json`: Session parameters for Pavlovian behavior experiments
- `fiber_photometry_session_params.json`: Session parameters for fiber photometry experiments

These files contain session-specific parameters like:
- `subject_id`: Subject identifier
- `experimenter_full_name`: List of experimenter names
- `rig_id`: Identifier for the experimental rig
- `task_version`: Version of the task
- `iacuc_protocol`: IACUC protocol identifier
- `mouse_platform_name`: Name of the mouse platform used
- `active_mouse_platform`: Whether the mouse platform was active
- `session_type`: Type of session
- `task_name`: Name of the task
- `notes`: Additional notes about the session
- `reward_delivery`: Description of reward delivery method
- `punishment_delivery`: Description of punishment delivery method

### Data Stream Configuration Files
- `pavlovian_behavior_data_streams.json`: Data stream configurations for behavior experiments
- `fiber_photometry_data_streams.json`: Data stream configurations for fiber photometry experiments

These files contain a list of data stream objects, each representing a collection of devices that are recorded simultaneously. Each data stream must include:

**Required Fields:**
- `stream_start_time`: Start time of the stream (set to `null` in config, will be automatically set to session start time)
- `stream_end_time`: End time of the stream (set to `null` in config, will be automatically set to session end time)
- `stream_modalities`: List of modalities for this stream (e.g., ["Behavior"] or ["FIB"])

**Optional Fields (depending on modality):**
- `camera_names`: List of camera names
- `daq_names`: List of DAQ device names
- `detectors`: List of detector configurations
- `light_sources`: List of light source configurations
- `fiber_connections`: List of fiber connection configurations
- `software`: List of software configurations
- `notes`: Additional notes about the data stream

**Note on Time Fields:**
When `stream_start_time` and `stream_end_time` are set to `null` in the configuration file, the ETL process will automatically:
1. Set `stream_start_time` to the session's start time
2. Set `stream_end_time` to the session's end time

This ensures that no `null` values are written to the database, while allowing you to configure the data streams without knowing the exact times in advance.

**Example Data Streams File:**
```json
[
  {
    "stream_start_time": null,
    "stream_end_time": null,
    "stream_modalities": ["Behavior"],
    "daq_names": ["Behavior DAQ"],
    "software": [
      {
        "name": "Bonsai",
        "version": "2.7.0",
        "parameters": {},
        "url": ""
      }
    ],
    "notes": "Behavior modality: Pavlovian conditioning"
  }
]
```

**Note:** For FIB modality, you must include `light_sources`, `detectors`, and `fiber_connections`.

## Usage

### Pavlovian Behavior

You can use these configuration files with the `pavlovian_session_build.py` script:

```bash
python scripts/pavlovian_session_build.py \
  --data-dir /path/to/data \
  --session-params examples/pavlovian_behavior_session_params.json \
  --data-streams examples/pavlovian_behavior_data_streams.json
```

You can also override specific parameters from the command line:

```bash
python scripts/pavlovian_session_build.py \
  --data-dir /path/to/data \
  --session-params examples/pavlovian_behavior_session_params.json \
  --data-streams examples/pavlovian_behavior_data_streams.json \
  --subject-id 123456 \
  --experimenter-full-name "John Doe" \
  --active-mouse-platform true
```

### Fiber Photometry

Similarly, you can use the `fiber_photometry_session_build.py` script for fiber photometry data:

```bash
python scripts/fiber_photometry_session_build.py \
  --data-dir /path/to/fiber/data \
  --session-params examples/fiber_photometry_session_params.json \
  --data-streams examples/fiber_photometry_data_streams.json
```

With command-line overrides:

```bash
python scripts/fiber_photometry_session_build.py \
  --data-dir /path/to/fiber/data \
  --session-params examples/fiber_photometry_session_params.json \
  --data-streams examples/fiber_photometry_data_streams.json \
  --subject-id 123456 \
  --experimenter-full-name "John Doe" \
  --active-mouse-platform true
```

## Parameter Resolution Order

Parameters are resolved in the following order (highest priority first):
1. Command-line arguments
2. Configuration files
3. Default values

If a required parameter is not specified in either the command line or configuration files, the script will raise an error. 