"""Module for creating Pavlovian Behavior session metadata.

This module implements a simple ETL pattern for creating session metadata,
with hooks for future extension to fetch additional data from external
services.
"""

import sys
import json
import glob
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union, Dict, Any, List, Optional, Tuple

from aind_data_schema.core.session import Session, StimulusEpoch
from aind_data_schema_models.modalities import Modality

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
                from datetime import timezone

                offset = -8 * 3600  # -08:00 timezone (Pacific)
                parsed_time = parsed_time.replace(
                    tzinfo=timezone(timedelta(seconds=offset))
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

        # Create stimulus epoch data
        stimulus_epochs = [
            {
                "stimulus_name": "Pavlovian",
                "stimulus_start_time": parsed_time,
                "stimulus_end_time": None,  # End time not available
                "trials_finished": int(trial_data["TrialNumber"].iloc[-1]),
                "trials_total": int(trial_data["TrialNumber"].iloc[-1]),
                "trials_rewarded": int(trial_data["TotalRewards"].iloc[-1]),
                "reward_consumed_during_epoch": int(
                    trial_data["TotalRewards"].iloc[-1]
                )
                * 2,  # Assuming 2 units per reward
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
                import logging

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
            stimulus_epoch = StimulusEpoch(
                stimulus_name=epoch_data.get("stimulus_name", "Pavlovian"),
                stimulus_start_time=epoch_data.get(
                    "stimulus_start_time", settings.session_start_time
                ),
                stimulus_end_time=epoch_data.get(
                    "stimulus_end_time", settings.session_end_time
                ),
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
        # Hook for service integrations - gather any additional optional fields
        # Example service calls:
        # - Query LIMS for subject procedure history using settings.subject_id
        # - Get configuration details from rig control system using settings.rig_id
        # - Fetch task parameters from a task database
        #
        # These calls should gather data for optional Session fields defined in the schema

        # Create stimulus epochs
        stimulus_epochs = self._create_stimulus_epochs(settings)

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
            task_name=settings.task_name,
            task_version=settings.task_version,
            stimulus_frame_rate=settings.stimulus_frame_rate,
            stimulus_epochs=stimulus_epochs,
            # Add any optional fields gathered from services here
        )

        return session

    def run_job(self) -> JobResponse:
        """Run the ETL job and return a JobResponse.

        Returns:
            JobResponse containing the session data
        """
        extracted = self._extract()
        transformed = self._transform(extracted)
        return self._load(transformed, self.job_settings.output_directory)


if __name__ == "__main__":  # pragma: no cover
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = BehaviorEtl(job_settings=main_job_settings)
    etl.run_job()
