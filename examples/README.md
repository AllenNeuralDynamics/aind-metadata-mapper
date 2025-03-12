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

These files contain a list of `Stream` objects, each representing a collection of devices that are recorded simultaneously. A session can have multiple streams if, for example, different devices are used at different times during the session.

**Required Fields for Each Stream:**
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

**Modality-Specific Requirements:**
- For `FIB` modality: Must include `light_sources`, `detectors`, and `fiber_connections`
- For `Behavior_Videos` modality: Must include `camera_names`
- For other modalities: See the schema for specific requirements

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

### Combined Experiments

For experiments that combine Fiber Photometry with Pavlovian Behavior, you can use the `fiber+pavlovian_session_build.py` script to generate a combined session file in one step:

```bash
python scripts/fiber+pavlovian_session_build.py \
  --data-dir /path/to/data \
  --pavlovian-session-params examples/pavlovian_behavior_session_params.json \
  --pavlovian-data-streams examples/pavlovian_behavior_data_streams.json \
  --fiber-session-params examples/fiber_photometry_session_params.json \
  --fiber-data-streams examples/fiber_photometry_data_streams.json \
  --subject-id 123456 \
  --experimenter-full-name "John Doe" \
  --active-mouse-platform true \
  --task-name "Pavlovian Conditioning"
```

This script will:
1. Run the Pavlovian behavior ETL process with the specified parameters and config files
2. Run the fiber photometry ETL process with the specified parameters and config files
3. Merge the resulting session files
4. Write the combined session to a new file

You can customize the output filenames:

```bash
python scripts/fiber+pavlovian_session_build.py \
  --data-dir /path/to/data \
  --pavlovian-session-params examples/pavlovian_behavior_session_params.json \
  --pavlovian-data-streams examples/pavlovian_behavior_data_streams.json \
  --fiber-session-params examples/fiber_photometry_session_params.json \
  --fiber-data-streams examples/fiber_photometry_data_streams.json \
  --pavlovian-output-filename session_pavlovian.json \
  --fiber-output-filename session_fiber.json \
  --combined-output-filename session_combined.json
```

The script will generate three output files:
1. The Pavlovian behavior session file
2. The fiber photometry session file
3. The combined session file that merges both modalities

If you need to merge existing session files, you can use the `merge_session_files.py` script directly:

```bash
python scripts/merge_session_files.py \
  --input-files /path/to/session_pavlovian_behavior.json /path/to/session_fiber_photometry.json \
  --output-file /path/to/session_combined.json
```

## Parameter Resolution Order

Parameters are resolved in the following order (highest priority first):
1. Command-line arguments
2. Configuration files
3. Default values

If a required parameter is not specified in either the command line or configuration files, the script will raise an error. 