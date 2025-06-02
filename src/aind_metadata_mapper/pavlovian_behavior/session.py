"""Module for creating Pavlovian Behavior session metadata.

This module implements an ETL (Extract, Transform, Load) pattern for generating
standardized session metadata from Pavlovian conditioning experiments. It
handles:

- Extraction of session times and trial data from behavior files
- Transformation of raw data into standardized session objects
- Loading/saving of session metadata in a standard format

The ETL class provides hooks for future extension to fetch additional data from
external services or handle new data formats.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Union, Optional, List
from dataclasses import dataclass
from datetime import datetime

from aind_data_schema.base import AindCoreModel
from aind_data_schema.core.session import (
    Session,
    StimulusEpoch,
    Software,
    SpeakerConfig,
)
from aind_data_schema.components.stimulus import AuditoryStimulation
from aind_data_schema.core.session import RewardDeliveryConfig

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings
from aind_metadata_mapper.pavlovian_behavior.utils import extract_session_data
from aind_metadata_mapper.utils.timing_utils import (
    validate_session_temporal_consistency,
)


@dataclass
class PavlovianData:
    """Intermediate data model for Pavlovian behavior data.

    This model holds the extracted and processed data before final
    transformation into a Session object. It serves as a structured
    intermediate representation of the session data.

    Parameters
    ----------
    start_time : datetime
        Session start time from behavior files
    end_time : datetime
        Session end time from behavior files
    stimulus_epochs : List[StimulusEpoch]
        List of stimulus epochs containing trial information
    reward_consumed_total : float
        Total reward consumed during session
    reward_consumed_unit : str
        Unit for reward measurement
    reward_delivery : RewardDeliveryConfig
        Configuration for reward delivery
    subject_id : str
        Subject identifier
    experimenter_full_name : List[str]
        List of experimenter names
    session_type : str
        Type of session (e.g. "Pavlovian")
    rig_id : str
        Identifier for the experimental rig
    iacuc_protocol : str
        IACUC protocol number
    notes : str
        Additional notes about the session
    mouse_platform_name : str
        Name of the mouse platform used
    active_mouse_platform : bool
        Whether the mouse platform was active
    data_streams : List[dict]
        Optional data stream configurations
    anaesthesia : Optional[str]
        Anaesthesia used
    animal_weight_post : Optional[float]
        Animal weight after session
    animal_weight_prior : Optional[float]
        Animal weight before session
    """

    start_time: datetime
    end_time: datetime
    stimulus_epochs: List[StimulusEpoch]
    reward_consumed_total: float
    reward_consumed_unit: str
    reward_delivery: RewardDeliveryConfig
    subject_id: str
    experimenter_full_name: List[str]
    session_type: str
    rig_id: str
    iacuc_protocol: str
    notes: str
    mouse_platform_name: str
    active_mouse_platform: bool
    data_streams: List[dict]
    anaesthesia: Optional[str]
    animal_weight_post: Optional[float]
    animal_weight_prior: Optional[float]


class ETL(GenericEtl[JobSettings]):
    """Creates Pavlovian behavior session metadata using an ETL pattern.

    This class handles the full lifecycle of session metadata creation:
    - Extracting timing and trial information from behavior files
    - Transforming raw data into standardized session objects
    - Loading/saving session metadata in a standard format

    The ETL process ensures that all required metadata fields are populated
    and validates the output against the AIND data schema.

    This class inherits from GenericEtl which provides the _load method
    for writing session metadata to a JSON file using a standard filename
    format (session_pavlovian.json).
    """

    def __init__(self, job_settings: Union[str, JobSettings]):
        """Initialize ETL with job settings.

        Parameters
        ----------
        job_settings : Union[str, JobSettings]
            Either a JobSettings object or a JSON string that can
            be parsed into one. The settings define all required parameters
            for the session metadata, including experimenter info, subject
            ID, data paths, etc.

        Raises
        ------
        ValidationError
            If the provided settings fail schema validation
        JSONDecodeError
            If job_settings is a string but not valid JSON
        """
        if isinstance(job_settings, str):
            job_settings = JobSettings(**json.loads(job_settings))
        super().__init__(job_settings)

    def _extract(self) -> PavlovianData:
        """Extract metadata from job settings and behavior files.

        This method validates the data directory and extracts session timing
        and trial information from behavior files. The extracted data is used
        to create a PavlovianData object containing all session information.

        Returns
        -------
        PavlovianData
            Intermediate data model containing all extracted and processed data

        Raises
        ------
        ValueError
            If required files are missing or data cannot be extracted
        """
        settings = self.job_settings
        logging.info("Starting metadata extraction")

        if (
            not hasattr(settings, "data_directory")
            or not settings.data_directory
        ):
            raise ValueError(
                "data_directory is required for metadata extraction"
            )

        try:
            data_dir = Path(settings.data_directory)
            reward_units = getattr(settings, "reward_units_per_trial", 2.0)
            local_timezone = getattr(settings, "local_timezone", None)
            session_time, stimulus_epochs = extract_session_data(
                data_dir, reward_units, local_timezone=local_timezone
            )

            # Merge user-supplied fields into extracted epochs
            stimulus_epochs = self._merge_user_epochs(
                settings, stimulus_epochs
            )

            # Post-process epochs (type conversions)
            stimulus_epochs = self._postprocess_epochs(stimulus_epochs)

            reward_consumed_total = self._calculate_reward_total(
                stimulus_epochs
            )

            data_streams = self._process_data_streams(
                settings, session_time, stimulus_epochs
            )

            # Create intermediate data model
            return PavlovianData(
                start_time=session_time,
                end_time=stimulus_epochs[0].stimulus_end_time,
                stimulus_epochs=stimulus_epochs,
                reward_consumed_total=reward_consumed_total,
                reward_consumed_unit=settings.reward_consumed_unit,
                reward_delivery=settings.reward_delivery,
                subject_id=settings.subject_id,
                experimenter_full_name=settings.experimenter_full_name,
                session_type=settings.session_type,
                rig_id=settings.rig_id,
                iacuc_protocol=settings.iacuc_protocol,
                notes=settings.notes,
                mouse_platform_name=settings.mouse_platform_name,
                active_mouse_platform=settings.active_mouse_platform,
                data_streams=data_streams,
                anaesthesia=settings.anaesthesia,
                animal_weight_post=settings.animal_weight_post,
                animal_weight_prior=settings.animal_weight_prior,
            )

        except (FileNotFoundError, ValueError) as e:
            raise ValueError(f"Failed to extract data from files: {str(e)}")

    def _merge_user_epochs(self, settings, stimulus_epochs):
        """Merge user-supplied epoch fields into extracted epochs."""
        if getattr(settings, "stimulus_epochs", None):
            user_epoch = settings.stimulus_epochs[
                0
            ]  # assuming one epoch for now
            for epoch in stimulus_epochs:
                for k, v in user_epoch.dict().items():
                    if v is not None:
                        setattr(epoch, k, v)
        return stimulus_epochs

    def _postprocess_epochs(self, stimulus_epochs):
        """
        Convert dicts to models for software,
        speaker_config, and stimulus_parameters.
        """
        for epoch in stimulus_epochs:
            # Convert software list of dicts to list of Software models
            if hasattr(epoch, "software") and epoch.software:
                epoch.software = [
                    Software(**s) if isinstance(s, dict) else s
                    for s in epoch.software
                ]
            # Convert speaker_config dict to SpeakerConfig model
            if (
                hasattr(epoch, "speaker_config")
                and epoch.speaker_config
                and isinstance(epoch.speaker_config, dict)
            ):
                epoch.speaker_config = SpeakerConfig(**epoch.speaker_config)
            # Convert all stimulus_parameters dicts to
            # AuditoryStimulation models
            if (
                hasattr(epoch, "stimulus_parameters")
                and epoch.stimulus_parameters
            ):
                epoch.stimulus_parameters = [
                    AuditoryStimulation(**p) if isinstance(p, dict) else p
                    for p in epoch.stimulus_parameters
                ]
        return stimulus_epochs

    def _calculate_reward_total(self, stimulus_epochs):
        """Sum reward_consumed_during_epoch across all epochs."""
        return sum(
            epoch.reward_consumed_during_epoch
            for epoch in stimulus_epochs
            if getattr(epoch, "reward_consumed_during_epoch", None) is not None
        )

    def _process_data_streams(self, settings, session_time, stimulus_epochs):
        """Process and fill in data_streams."""
        data_streams = []
        if hasattr(settings, "data_streams") and settings.data_streams:
            for stream in settings.data_streams:
                if stream.get("stream_start_time") is None:
                    stream["stream_start_time"] = session_time
                if stream.get("stream_end_time") is None:
                    stream["stream_end_time"] = stimulus_epochs[
                        0
                    ].stimulus_end_time
                data_streams.append(stream)
        return data_streams

    def _transform(self, pavlovian_data: PavlovianData) -> Session:
        """Transform extracted data into a valid Session object.

        Parameters
        ----------
        pavlovian_data : PavlovianData
            Intermediate data model containing all session information

        Returns
        -------
        Session
            A fully configured Session object that conforms to the
            AIND data schema

        Notes
        -----
        Creates a standardized Session object from the intermediate data model,
        including all stimulus epochs and their configurations.
        """
        # Use datetime objects directly with microseconds removed
        start_time = pavlovian_data.start_time.replace(microsecond=0)
        end_time = pavlovian_data.end_time.replace(microsecond=0)

        session = Session(
            experimenter_full_name=pavlovian_data.experimenter_full_name,
            session_start_time=start_time,
            session_end_time=end_time,
            session_type=pavlovian_data.session_type,
            rig_id=pavlovian_data.rig_id,
            subject_id=pavlovian_data.subject_id,
            iacuc_protocol=pavlovian_data.iacuc_protocol,
            notes=pavlovian_data.notes,
            mouse_platform_name=pavlovian_data.mouse_platform_name,
            active_mouse_platform=pavlovian_data.active_mouse_platform,
            data_streams=pavlovian_data.data_streams,
            stimulus_epochs=pavlovian_data.stimulus_epochs,
            reward_consumed_total=pavlovian_data.reward_consumed_total,
            reward_consumed_unit=pavlovian_data.reward_consumed_unit,
            reward_delivery=pavlovian_data.reward_delivery,
            anaesthesia=pavlovian_data.anaesthesia,
            animal_weight_post=pavlovian_data.animal_weight_post,
            animal_weight_prior=pavlovian_data.animal_weight_prior,
        )

        # Validate temporal consistency (end time > start time)
        validate_session_temporal_consistency(session)

        return session

    def _load(
        self, output_model: AindCoreModel, output_directory: Optional[Path]
    ) -> JobResponse:
        """Override parent _load to handle custom filenames and
        default directories.

        This implementation differs from the parent GenericEtl._load
        in that it:
        1. Uses the filename specified in job_settings rather than model's
           default
        2. Falls back to data_directory if no output_directory specified
        3. Maintains validation and error handling from parent class

        Parameters
        ----------
        output_model : AindCoreModel
            The final model that has been constructed and validated
        output_directory : Optional[Path]
            Directory where the file should be written. If None,
            defaults to job_settings.data_directory

        Returns
        -------
        JobResponse
            Object containing status code, message, and optional data.
            Status codes:
            - 200: Success
            - 500: File writing errors
        """
        # If no output directory specified, use the data directory
        if output_directory is None:
            output_directory = Path(self.job_settings.data_directory)

        output_path = output_directory / self.job_settings.output_filename
        with open(output_path, "w") as f:
            f.write(output_model.model_dump_json(indent=2))
        return JobResponse(
            status_code=200, message=f"Write model to {output_path}"
        )

    def run_job(self) -> JobResponse:
        """Run the complete ETL job and return a JobResponse.

        This method orchestrates the full ETL process:
        1. Extracts metadata from files and settings
        2. Transforms the data into a valid Session object
        3. Saves the session metadata to the specified output location

        Returns
        -------
        JobResponse
            Object containing status code, message, and optional data.
            Status codes:
            - 200: Success
            - 406: Validation errors
            - 500: File writing errors

        Notes
        -----
        Uses the parent class's _load method which handles validation and
        writing.
        """
        pavlovian_data = self._extract()
        transformed = self._transform(pavlovian_data)
        job_response = self._load(
            transformed, self.job_settings.output_directory
        )
        return job_response


if __name__ == "__main__":  # pragma: no cover
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = ETL(job_settings=main_job_settings)
    etl.run_job()
