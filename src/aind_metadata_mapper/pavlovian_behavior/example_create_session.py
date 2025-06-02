"""
Simple script to create Pavlovian behavior metadata with default settings.

User should use this script to create a new session metadata file,
modifying specific fields as needed.

Example command to run the script from the command line:

```bash
python src/aind_metadata_mapper/pavlovian_behavior/example_create_session.py \
    --subject-id 000000 \
    --data-directory data/sample_fiber_data \
    --output-directory data/sample_fiber_data \
    --output-filename session_pavlovian_behavior.json
```
=======
"""

from pathlib import Path
from typing import List, Optional
import logging
import sys

from aind_metadata_mapper.pavlovian_behavior.session import ETL
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import VolumeUnit
from aind_data_schema.core.session import (
    RewardDeliveryConfig,
    RewardSolution,
    RewardSpoutConfig,
    SpoutSide,
    RelativePosition,
)
from aind_data_schema.components.coordinates import (
    Axis,
    AxisName,
    Translation3dTransform,
)


def create_reward_delivery_config() -> RewardDeliveryConfig:
    """Create the reward delivery configuration for Pavlovian conditioning.

    Returns
    -------
    RewardDeliveryConfig
        Configuration specifying water delivery
        from left spout with right spout retracted.
        Uses placeholders for coordinates since
        they are manually adjusted.
    """
    return RewardDeliveryConfig(
        reward_solution=RewardSolution.WATER,
        reward_spouts=[
            RewardSpoutConfig(
                side=SpoutSide.LEFT,
                starting_position=RelativePosition(
                    device_position_transformations=[
                        Translation3dTransform(translation=[0.0, -1.5, 0.0]),
                    ],
                    device_origin=("lower jaw"),
                    device_axes=[
                        Axis(name=AxisName.X, direction="left/right"),
                        Axis(name=AxisName.Y, direction="rostro-caudal"),
                        Axis(name=AxisName.Z, direction="up/down"),
                    ],
                    notes=(
                        "Left spout in extended position, manually positioned "
                        "about 1.5 mm from the lower jaw"
                    ),
                ),
                variable_position=False,
            ),
            RewardSpoutConfig(
                side=SpoutSide.RIGHT,
                starting_position=RelativePosition(
                    device_position_transformations=[
                        Translation3dTransform(translation=[5.0, -31.5, 0.0]),
                    ],
                    device_origin=("lower jaw"),
                    device_axes=[
                        Axis(name=AxisName.X, direction="left/right"),
                        Axis(name=AxisName.Y, direction="rostro-caudal"),
                        Axis(name=AxisName.Z, direction="up/down"),
                    ],
                    notes=(
                        "Right spout in retracted position, "
                        "retraction travel is 30 mm "
                        "placing it about 31.5 mm from the lower jaw."
                    ),
                ),
                variable_position=False,
            ),
        ],
        notes="Water delivered from left lick spout, right spout retracted",
    )


def create_metadata(
    subject_id: str,
    data_directory: Path,
    output_directory: Path,
    output_filename: str = "session_pavlovian.json",
    experimenter_full_name: List[str] = [
        "test_experimenter_1",
        "test_experimenter_2",
    ],
    rig_id: str = "428_9_0_20240617",
    iacuc_protocol: str = "2115",
    mouse_platform_name: str = "mouse_tube_foraging",
    active_mouse_platform: bool = False,
    session_type: str = "Pavlovian_Conditioning",
    notes: str = (
        "The following information has no corresponding field "
        "in the session metadata: "
        "Punishment delivery: airpuff with 25 psi for 1 s (DO2)."
    ),
    reward_delivery: Optional[RewardDeliveryConfig] = None,
    reward_units_per_trial: float = 2.0,
    reward_consumed_unit: VolumeUnit = VolumeUnit.UL,
    local_timezone: str = "America/Los_Angeles",
    anaesthesia: Optional[str] = None,
    animal_weight_post: Optional[float] = None,
    animal_weight_prior: Optional[float] = None,
) -> bool:
    """Create Pavlovian behavior metadata with default settings.

    Args:
        subject_id: Subject identifier
        data_directory: Path to behavior data directory
        output_directory: Directory where metadata will be saved
        output_filename: Name of the output JSON file
        experimenter_full_name: List of experimenter names
        rig_id: Identifier for the experimental rig
        iacuc_protocol: Protocol identifier
        mouse_platform_name: Name of the mouse platform
        active_mouse_platform: Whether platform is active
        session_type: Type of experimental session
        notes: Additional notes about the session
        reward_units_per_trial: Number of reward units per successful trial
        reward_consumed_unit: Unit of reward consumed
        local_timezone: Local timezone (defaults to Pacific timezone if None)
        anaesthesia: Anaesthesia used
        animal_weight_post: Animal weight after session
        animal_weight_prior: Animal weight before session
        reward_delivery: Optional[RewardDeliveryConfig]
            Configuration for reward delivery. If None,
            defaults to water from left spout
            with right spout retracted.

    Returns:
        bool: True if metadata was successfully created and verified
    """
    # Create settings with defaults
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
        "reward_units_per_trial": reward_units_per_trial,
        "reward_consumed_unit": reward_consumed_unit,
        "local_timezone": local_timezone,
        "anaesthesia": anaesthesia,
        "animal_weight_post": animal_weight_post,
        "animal_weight_prior": animal_weight_prior,
        "reward_delivery": reward_delivery or create_reward_delivery_config(),
        "data_streams": [
            {
                "stream_start_time": None,
                "stream_end_time": None,
                "stream_modalities": [
                    Modality.BEHAVIOR_VIDEOS,
                    Modality.BEHAVIOR,
                ],
                "camera_names": [
                    "BehaviorVideography_Eye",
                    "BehaviorVideography_Body",
                ],
                "daq_names": [""],
                "light_sources": [
                    {
                        "device_type": "Light emitting diode",
                        "excitation_power": None,
                        "excitation_power_unit": "milliwatt",
                        "name": "IR LED",
                    }
                ],
                "notes": "Behavioral tracking with IR LED",
                "software": [
                    {
                        "name": "Bonsai",
                        "parameters": {},
                        "url": "https://github.com/AllenNeuralDynamics/PavlovianCond_Bonsai/tree/dafd7dfe0f347f781e91466b3d16b83cf32f8b6d",  # noqa E501
                        "version": "",
                    }
                ],
            }
        ],
        "stimulus_epochs": [
            {
                "stimulus_name": "CS - auditory conditioned stimuli",
                "stimulus_parameters": [
                    {
                        "amplitude_modulation_frequency": 5000,
                        "frequency_unit": "hertz",
                        "sample_frequency": "96000",
                        "stimulus_name": "CS1",
                        "stimulus_type": "Auditory Stimulation",
                        "notes": (
                            "assigned reward probability is 10%, "
                            "duration is 1.0 s"
                        ),
                    },
                    {
                        "amplitude_modulation_frequency": 8000,
                        "frequency_unit": "hertz",
                        "sample_frequency": "96000",
                        "stimulus_name": "CS2",
                        "stimulus_type": "Auditory Stimulation",
                        "notes": (
                            "assigned reward probability is 50%, "
                            "duration is 1.0 s"
                        ),
                    },
                    {
                        "amplitude_modulation_frequency": 13000,
                        "frequency_unit": "hertz",
                        "sample_frequency": "96000",
                        "stimulus_name": "CS3",
                        "stimulus_type": "Auditory Stimulation",
                        "notes": (
                            "assigned reward probability is 90%, "
                            "duration is 1.0 s"
                        ),
                    },
                    {
                        "bandpass_filter_type": None,
                        "frequency_unit": "hertz",
                        "sample_frequency": "96000",
                        "stimulus_name": "CS4",
                        "stimulus_type": "Auditory Stimulation",
                        "notes": (
                            "White noise, aversive probability is 90%, "
                            "duration is 1.0 s"
                        ),
                    },
                ],
                "speaker_config": {
                    "name": "Stimulus Speaker",
                    "volume": "72",
                    "volume_unit": "decibels",
                },
                "stimulus_device_names": ["Stimulus Speaker"],
                "software": [
                    {
                        "name": "Bonsai",
                        "parameters": {},
                        "url": "https://github.com/AllenNeuralDynamics/PavlovianCond_Bonsai/tree/dafd7dfe0f347f781e91466b3d16b83cf32f8b6d",  # noqa E501
                        "version": "",
                    }
                ],
                # Do NOT include: stimulus_start_time, stimulus_end_time,
                # reward_consumed_during_epoch, trials_total, etc.
                # These are populated by the ETL from the behavior files.
            }
        ],
    }

    # Create JobSettings instance and run ETL
    job_settings = JobSettings(**settings)
    etl = ETL(job_settings)
    response = etl.run_job()

    if response.status_code != 200:
        logging.error(f"ETL job failed: {response.message}")
        return False

    return True  # If we get here, ETL job succeeded and file was verified


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Create Pavlovian behavior metadata with default settings"
    )
    parser.add_argument(
        "--subject-id", type=str, required=True, help="Subject identifier"
    )
    parser.add_argument(
        "--data-directory",
        type=Path,
        required=True,
        help="Path to behavior data directory",
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
        default="session_pavlovian.json",
        help="Name of the output JSON file (default: session_pavlovian.json)",
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
        sys.exit(1)
