"""
Simple script to create fiber photometry metadata with default settings.

Example command to run the script from the command line,
assuming data is in data/sample_fiber_data relative to the root of the repo:

```bash
python src/aind_metadata_mapper/fip/example_create_session.py \
    --subject-id 000000 \
    --data-directory data/sample_fiber_data \
    --output-directory data/sample_fiber_data \
    --output-filename session_fip.json
```
=======
Example python code which could be saved to some local file and run from the
command line with a simple `python <filename>.py`

```python
from pathlib import Path
from aind_metadata_mapper.fip.example_create_session import create_metadata

create_metadata(
    subject_id="000000",
    data_directory=Path(
        r"/Users/doug.ollerenshaw/code/aind-metadata-mapper/data/sample_fiber_data"
    ),
    output_directory=Path(
        r"/Users/doug.ollerenshaw/code/aind-metadata-mapper/data/sample_fiber_data"
    ),
    output_filename="session_fip.json",
    # Optional parameters with defaults:
    experimenter_full_name=["test_experimenter_1", "test_experimenter_2"],
    rig_id="428_9_B_20240617",
    iacuc_protocol="2115",
    mouse_platform_name="mouse_tube_foraging",
    active_mouse_platform=False,
    session_type="Foraging_Photometry",
    task_name="Fiber Photometry",
    notes="Example configuration for fiber photometry rig",
)
```

"""

from pathlib import Path
from typing import List, Optional
import logging
import sys

from aind_metadata_mapper.fip.session import FIBEtl
from aind_metadata_mapper.fip.models import JobSettings


def create_metadata(
    subject_id: str,
    data_directory: Path,
    output_directory: Optional[Path] = None,
    output_filename: str = "session_fip.json",
    experimenter_full_name: List[str] = [
        "test_experimenter_1",
        "test_experimenter_2",
    ],
    rig_id: str = "428_9_0_20240617",
    iacuc_protocol: str = "2115",
    mouse_platform_name: str = "mouse_tube_foraging",
    active_mouse_platform: bool = False,
    session_type: str = "FIB",
    notes: str = "",
    local_timezone: str = "America/Los_Angeles",
    anaesthesia: Optional[str] = None,
    animal_weight_post: Optional[float] = None,
    animal_weight_prior: Optional[float] = None,
) -> bool:
    """Create fiber photometry metadata with default settings.

    Args:
        subject_id: Subject identifier
        data_directory: Path to fiber photometry data directory
        output_directory: Directory where metadata will be saved
        output_filename: Name of the output JSON file
        experimenter_full_name: List of experimenter names
        rig_id: Identifier for the experimental rig
        iacuc_protocol: Protocol identifier
        mouse_platform_name: Name of the mouse platform
        active_mouse_platform: Whether platform is active
        session_type: Type of experimental session
        notes: Additional notes about the session
        local_timezone: Local timezone (defaults to Pacific timezone if None)
        anaesthesia: Anaesthesia used
        animal_weight_post: Animal weight after session
        animal_weight_prior: Animal weight before session

    Returns:
        bool: True if metadata was successfully
            created and verified, False otherwise
    """
    # Create settings with defaults for stream configuration
    settings = {
        "subject_id": subject_id,
        "experimenter_full_name": experimenter_full_name,
        "data_directory": str(data_directory),
        "output_directory": str(output_directory),
        "output_filename": output_filename,
        "rig_id": rig_id,
        "iacuc_protocol": iacuc_protocol,
        "mouse_platform_name": mouse_platform_name,
        "active_mouse_platform": active_mouse_platform,
        "session_type": session_type,
        "notes": notes,
        "local_timezone": local_timezone,
        "anaesthesia": anaesthesia,
        "animal_weight_post": animal_weight_post,
        "animal_weight_prior": animal_weight_prior,
        "data_streams": [
            {
                "stream_start_time": None,
                "stream_end_time": None,
                "stream_modalities": ["FIB"],
                "camera_names": [],
                "daq_names": [""],
                "detectors": [
                    {
                        "exposure_time": "15650",
                        "exposure_time_unit": "microsecond",
                        "name": "Green CMOS",
                        "trigger_type": "Internal",
                    },
                    {
                        "exposure_time": "15650",
                        "exposure_time_unit": "microsecond",
                        "name": "Red CMOS",
                        "trigger_type": "Internal",
                    },
                ],
                "ephys_modules": [],
                "fiber_connections": [
                    {
                        "fiber_name": "Fiber 0",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord 0",
                        "patch_cord_output_power": "20",
                    },
                    {
                        "fiber_name": "Fiber 1",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord 1",
                        "patch_cord_output_power": "20",
                    },
                    {
                        "fiber_name": "Fiber 2",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord 2",
                        "patch_cord_output_power": "20",
                    },
                    {
                        "fiber_name": "Fiber 3",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord 3",
                        "patch_cord_output_power": "20",
                    },
                ],
                "fiber_modules": [],
                "light_sources": [
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": 20,
                        "excitation_power_unit": "microwatt",
                        "name": "470nm LED",
                    },
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": 20,
                        "excitation_power_unit": "microwatt",
                        "name": "415nm LED",
                    },
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": 20,
                        "excitation_power_unit": "microwatt",
                        "name": "565nm LED",
                    },
                ],
                "manipulator_modules": [],
                "mri_scans": [],
                "notes": "Fib modality: fib mode: Normal",
                "ophys_fovs": [],
                "slap_fovs": [],
                "software": [
                    {
                        "name": "Bonsai",
                        "parameters": {},
                        "url": "https://github.com/AllenNeuralDynamics/PavlovianCond_Bonsai/tree/dafd7dfe0f347f781e91466b3d16b83cf32f8b6d",  # noqa E501
                        "version": "",
                    }
                ],
                "stack_parameters": None,
                "stick_microscopes": [],
            }
        ],
    }

    # Create JobSettings instance and run ETL
    job_settings = JobSettings(**settings)
    etl = FIBEtl(job_settings)
    response = etl.run_job()

    if response.status_code != 200:
        logging.error(f"ETL job failed: {response.message}")
        return False

    return True  # If we get here, ETL job succeeded and file was verified


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Create fiber photometry metadata with default settings"
    )
    parser.add_argument(
        "--subject-id", type=str, required=True, help="Subject identifier"
    )
    parser.add_argument(
        "--data-directory",
        type=Path,
        required=True,
        help="Path to fiber photometry data directory",
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        help="Directory where metadata will be saved (default: same as data directory)",  # noqa E501
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_fip.json",
        help="Name of the output JSON file (default: session_fip.json)",
    )
    parser.add_argument(
        "--active-mouse-platform",
        action="store_true",
        help="Whether the mouse platform was active",
    )
    parser.add_argument(
        "--local-timezone",
        type=str,
        default="America/Los_Angeles",
        help="Local timezone",
    )
    parser.add_argument(
        "--anaesthesia", type=str, default=None, help="Anaesthesia used"
    )
    parser.add_argument(
        "--animal-weight-post",
        type=float,
        default=None,
        help="Animal weight after session",
    )
    parser.add_argument(
        "--animal-weight-prior",
        type=float,
        default=None,
        help="Animal weight before session",
    )
    parser.add_argument(
        "--mouse-platform-name",
        type=str,
        default="mouse_tube_foraging",
        help="Name of the mouse platform",
    )

    args = parser.parse_args()

    success = create_metadata(
        subject_id=args.subject_id,
        data_directory=args.data_directory,
        output_directory=args.output_directory,
        output_filename=args.output_filename,
        active_mouse_platform=args.active_mouse_platform,
        local_timezone=args.local_timezone,
        anaesthesia=args.anaesthesia,
        animal_weight_post=args.animal_weight_post,
        animal_weight_prior=args.animal_weight_prior,
        mouse_platform_name=args.mouse_platform_name,
    )

    output_path = args.output_directory / args.output_filename
    if success:
        print(f"Metadata successfully saved and verified at: {output_path}")
    else:
        print(f"Failed to create or verify metadata at: {output_path}")
        sys.exit(1)  # Exit with error code if verification fails
