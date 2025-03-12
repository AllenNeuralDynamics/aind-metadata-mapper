"""Module for creating Pavlovian Behavior session metadata.

This module implements a simple ETL pattern for creating session metadata,
with hooks for future extension to fetch additional data from external
services.
"""

import sys
import json
import glob
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, Dict, Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

from aind_data_schema.core.session import Session, StimulusEpoch
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import VolumeUnit

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings


class BehaviorEtl(GenericEtl[JobSettings]):
    """Creates Pavlovian behavior session metadata with ETL pattern."""

    def __init__(self, job_settings: Union[str, JobSettings]):
        """Initialize ETL with job settings.

        Args:
            job_settings: Either a JobSettings object or a JSON string that can
                be parsed into one
        """
        if isinstance(job_settings, str):
            job_settings = JobSettings(**json.loads(job_settings))
        super().__init__(job_settings)

    def _extract_data_from_files(
        self, data_dir: Union[str, Path]
    ) -> Tuple[datetime, List[Dict[str, Any]]]:
        """Extract session data from behavior files.

        Args:
            data_dir: Path to directory containing behavior files

        Returns:
            Tuple containing session start time and stimulus epochs data
        """
        data_dir = Path(data_dir)
        behavior_dir = data_dir / "behavior"

        if not behavior_dir.exists():
            behavior_dir = (
                data_dir  # Try using the provided directory directly
            )

        # Find behavior files
        behavior_files = list(behavior_dir.glob("TS_CS1_*.csv"))
        if not behavior_files:
            raise FileNotFoundError(
                f"No behavior files found in {behavior_dir}"
            )

        # Extract session start time from filename
        behavior_file = behavior_files[0]
        raw_time_part = "_".join(behavior_file.stem.split("_")[2:])

        try:
            parsed_time = datetime.strptime(raw_time_part, "%Y-%m-%dT%H_%M_%S")
            # Add timezone info if needed
            if parsed_time.tzinfo is None:
                parsed_time = parsed_time.replace(
                    tzinfo=ZoneInfo("America/Los_Angeles")
                )
        except ValueError:
            raise ValueError(
                f"Could not parse datetime from filename: {behavior_file.name}"
            )

        # Find trial files
        trial_files = list(behavior_dir.glob("TrialN_TrialType_ITI_*.csv"))
        if not trial_files:
            raise FileNotFoundError(f"No trial files found in {behavior_dir}")

        trial_file = trial_files[0]
        trial_data = pd.read_csv(trial_file)

        # Calculate session duration from ITI values
        total_session_duration = trial_data["ITI_s"].sum()

        # Calculate end time based on actual session duration
        end_time = parsed_time + timedelta(
            seconds=float(total_session_duration)
        )

        # Create stimulus epoch data
        stimulus_epochs = [
            {
                "stimulus_start_time": parsed_time,
                "stimulus_end_time": end_time,
                "stimulus_modalities": ["Auditory"],
                "trials_finished": int(trial_data["TrialNumber"].iloc[-1]),
                "trials_total": int(trial_data["TrialNumber"].iloc[-1]),
                "trials_rewarded": int(trial_data["TotalRewards"].iloc[-1]),
                "reward_consumed_during_epoch": int(
                    trial_data["TotalRewards"].iloc[-1]
                )
                * float(
                    getattr(self.job_settings, "reward_units_per_trial", 2.0)
                ),  # Default to 2 units per reward if not specified
            }
        ]

        return parsed_time, stimulus_epochs

    def _extract(self) -> JobSettings:
        """Extract metadata from job settings and external sources.

        Returns:
            JobSettings object with extracted data
        """
        settings = self.job_settings

        # Extract data from files if data_directory is provided
        if hasattr(settings, "data_directory") and settings.data_directory:
            try:
                session_time, stimulus_epochs = self._extract_data_from_files(
                    settings.data_directory
                )

                # Update settings with extracted data
                if settings.session_start_time is None:
                    settings.session_start_time = session_time

                # Set session_end_time if it's not already set
                if settings.session_end_time is None and stimulus_epochs:
                    # Use the end time from the first stimulus epoch
                    settings.session_end_time = stimulus_epochs[0].get(
                        "stimulus_end_time"
                    )

                # Only update stimulus_epochs if it's empty
                if not settings.stimulus_epochs:
                    settings.stimulus_epochs = stimulus_epochs

                # Add folder name to notes
                folder_name = os.path.basename(Path(settings.data_directory))
                if settings.notes:
                    settings.notes += f"\nOriginal folder name: {folder_name}"
                else:
                    settings.notes = f"Original folder name: {folder_name}"

            except (FileNotFoundError, ValueError) as e:

                logging.warning(f"Could not extract data from files: {e}")
                # Continue with provided settings

        # Ensure required fields are present
        if settings.session_start_time is None:
            raise ValueError(
                "session_start_time is required but was not provided or extracted"
            )

        return settings

    def _create_stimulus_epochs(
        self, settings: JobSettings
    ) -> List[StimulusEpoch]:
        """Create stimulus epochs from settings.

        Args:
            settings: Job settings containing stimulus epoch data

        Returns:
            List of StimulusEpoch objects
        """
        stimulus_epochs = []

        for epoch_data in settings.stimulus_epochs:
            # Get start time, defaulting to session start time
            start_time = epoch_data.get(
                "stimulus_start_time", settings.session_start_time
            )
            if start_time is None:
                raise ValueError(
                    "stimulus_start_time is required but was not provided"
                )

            # Get end time, which should already be calculated in _extract_data_from_files
            end_time = epoch_data.get(
                "stimulus_end_time", settings.session_end_time
            )
            if end_time is None:
                raise ValueError(
                    "stimulus_end_time is required but was not provided or calculated"
                )

            stimulus_epoch = StimulusEpoch(
                stimulus_name=epoch_data.get("stimulus_name", "Pavlovian"),
                stimulus_start_time=start_time,
                stimulus_end_time=end_time,
                stimulus_modalities=["Auditory"],  # Hardcoded as Auditory
                trials_finished=epoch_data.get("trials_finished", 0),
                trials_total=epoch_data.get("trials_total", 0),
                trials_rewarded=epoch_data.get("trials_rewarded", 0),
                reward_consumed_during_epoch=epoch_data.get(
                    "reward_consumed_during_epoch", 0
                ),
            )
            stimulus_epochs.append(stimulus_epoch)

        return stimulus_epochs

    def _transform(self, settings: JobSettings) -> Session:
        """Transform extracted data into a valid Session object.

        Args:
            settings: Job settings containing session data

        Returns:
            Session object with transformed data
        """
        # Create stimulus epochs
        stimulus_epochs = self._create_stimulus_epochs(settings)

        # Calculate total reward consumed across all stimulus epochs
        reward_consumed_total = sum(
            epoch.reward_consumed_during_epoch for epoch in stimulus_epochs
        )

        # Process data streams if they exist
        data_streams = []
        if hasattr(settings, "data_streams") and settings.data_streams:
            for stream in settings.data_streams:
                # Set stream_start_time and stream_end_time if they are null
                if stream.get("stream_start_time") is None:
                    stream["stream_start_time"] = settings.session_start_time
                if stream.get("stream_end_time") is None:
                    stream["stream_end_time"] = settings.session_end_time
                data_streams.append(stream)

        # Create session with all available data
        session = Session(
            experimenter_full_name=settings.experimenter_full_name,
            session_start_time=settings.session_start_time,
            session_end_time=settings.session_end_time,
            session_type=settings.session_type,
            rig_id=settings.rig_id,
            subject_id=settings.subject_id,
            iacuc_protocol=settings.iacuc_protocol,
            notes=settings.notes,
            mouse_platform_name=settings.mouse_platform_name,
            active_mouse_platform=settings.active_mouse_platform,
            data_streams=data_streams,  # Use processed data_streams
            stimulus_epochs=stimulus_epochs,
            reward_consumed_total=reward_consumed_total,
            reward_consumed_unit=VolumeUnit.UL,  # Using the proper enum value for microliters
        )

        return session

    def run_job(self) -> JobResponse:
        """Run the ETL job and return a JobResponse.

        Returns:
            JobResponse containing the session data
        """
        extracted = self._extract()
        transformed = self._transform(extracted)

        # Get the output directory and filename from settings
        output_directory = getattr(extracted, "output_directory", None)
        output_filename = getattr(extracted, "output_filename", None)

        # Convert to Path if it's a string
        if output_directory and isinstance(output_directory, str):

            output_directory = Path(output_directory)

        # If we have a custom filename, write the file directly
        if output_directory and output_filename:
            # Write the file with custom filename
            output_path = os.path.join(output_directory, output_filename)

            # Ensure output directory exists
            os.makedirs(output_directory, exist_ok=True)

            # Write the file manually to ensure the correct filename is used
            with open(output_path, "w") as f:
                f.write(transformed.model_dump_json(indent=2))

            return JobResponse(
                status_code=200,
                message=f"Session metadata saved to: {output_path}",
                data=None,
            )

        # Otherwise use the standard _load method
        return self._load(transformed, output_directory)


if __name__ == "__main__":  # pragma: no cover
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = BehaviorEtl(job_settings=main_job_settings)
    etl.run_job()
