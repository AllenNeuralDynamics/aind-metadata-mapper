"""Simple script to create fiber photometry metadata with default settings."""

from pathlib import Path
from typing import List

from aind_metadata_mapper.fib.session import ETL
from aind_metadata_mapper.fib.models import JobSettings


def create_metadata(
    subject_id: str,
    data_directory: Path,
    output_directory: Path,
    output_filename: str = "session_fib.json",
    experimenter_full_name: List[str] = [
        "test_experimenter_1",
        "test_experimenter_2",
    ],
    rig_id: str = "428_9_A_20240617",
    task_version: str = "1.0.0",
    iacuc_protocol: str = "2115",
    mouse_platform_name: str = "mouse_tube_foraging",
    active_mouse_platform: bool = False,
    session_type: str = "Foraging_Photometry",
    task_name: str = "Fiber Photometry",
    notes: str = "Example configuration for fiber photometry rig",
) -> None:
    """Create fiber photometry metadata with default settings.

    Args:
        subject_id: Subject identifier
        data_directory: Path to fiber photometry data directory
        output_directory: Directory where metadata will be saved
        output_filename: Name of the output JSON file
        experimenter_full_name: List of experimenter names
        rig_id: Identifier for the experimental rig
        task_version: Version of the experimental task
        iacuc_protocol: Protocol identifier
        mouse_platform_name: Name of the mouse platform
        active_mouse_platform: Whether platform is active
        session_type: Type of experimental session
        task_name: Name of the experimental task
        notes: Additional notes about the session
    """
    # Create settings with defaults for stream configuration
    settings = {
        "subject_id": subject_id,
        "experimenter_full_name": experimenter_full_name,
        "data_directory": str(data_directory),
        "output_directory": str(output_directory),
        "output_filename": output_filename,
        "rig_id": rig_id,
        "task_version": task_version,
        "iacuc_protocol": iacuc_protocol,
        "mouse_platform_name": mouse_platform_name,
        "active_mouse_platform": active_mouse_platform,
        "session_type": session_type,
        "task_name": task_name,
        "notes": notes,
        "data_streams": [
            {
                "stream_start_time": None,
                "stream_end_time": None,
                "stream_modalities": ["FIB"],
                "camera_names": [],
                "daq_names": [""],
                "detectors": [
                    {
                        "exposure_time": "5230.42765",
                        "exposure_time_unit": "millisecond",
                        "name": "Green CMOS",
                        "trigger_type": "Internal",
                    },
                    {
                        "exposure_time": "5230.42765",
                        "exposure_time_unit": "millisecond",
                        "name": "Red CMOS",
                        "trigger_type": "Internal",
                    },
                ],
                "ephys_modules": [],
                "fiber_connections": [
                    {
                        "fiber_name": "Fiber 0",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord A",
                        "patch_cord_output_power": "20",
                    },
                    {
                        "fiber_name": "Fiber 1",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord B",
                        "patch_cord_output_power": "20",
                    },
                    {
                        "fiber_name": "Fiber 2",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord C",
                        "patch_cord_output_power": "20",
                    },
                    {
                        "fiber_name": "Fiber 3",
                        "output_power_unit": "microwatt",
                        "patch_cord_name": "Patch Cord D",
                        "patch_cord_output_power": "20",
                    },
                ],
                "fiber_modules": [],
                "light_sources": [
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": None,
                        "excitation_power_unit": "milliwatt",
                        "name": "IR LED",
                    },
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": None,
                        "excitation_power_unit": "milliwatt",
                        "name": "470nm LED",
                    },
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": None,
                        "excitation_power_unit": "milliwatt",
                        "name": "415nm LED",
                    },
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": None,
                        "excitation_power_unit": "milliwatt",
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
                        "url": "",
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
    etl = ETL(job_settings)
    etl.run_job()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Create fiber photometry metadata with default settings"
    )
    parser.add_argument("subject_id", type=str, help="Subject identifier")
    parser.add_argument(
        "data_directory",
        type=Path,
        help="Path to fiber photometry data directory",
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path.cwd(),
        help=(
            "Directory where metadata will be saved "
            "(default: current directory)"
        ),
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="session_fib.json",
        help="Name of the output JSON file (default: session_fib.json)",
    )

    args = parser.parse_args()

    create_metadata(
        subject_id=args.subject_id,
        data_directory=args.data_directory,
        output_directory=args.output_directory,
        output_filename=args.output_filename,
    )
    print(f"Metadata saved to: {args.output_directory / args.output_filename}")
