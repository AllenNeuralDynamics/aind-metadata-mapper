"""Module to gather metadata from different sources."""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.metadata import Metadata, create_metadata_json
from aind_data_schema.core.model import Model
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import Processing
from aind_data_schema.core.quality_control import QualityControl
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.organizations import Organization
from pydantic import ValidationError

from aind_metadata_mapper.base import MapperJobSettings
from aind_metadata_mapper.mapper_registry import registry
from aind_metadata_mapper.models import JobSettings
from aind_metadata_mapper.utils import get_procedures, get_subject, metadata_service_helper, normalize_utc_timezone


class GatherMetadataJob:
    """Class to handle retrieving metadata"""

    def __init__(self, settings: JobSettings):
        """
        Class constructor
        Parameters
        ----------
        settings : JobSettings
        """
        self.settings = settings
        # Create output directory if it doesn't exist
        os.makedirs(self.settings.output_dir, exist_ok=True)

    def _does_file_exist_in_user_defined_dir(self, file_name: str) -> bool:
        """
        Check whether a file exists in a directory.
        Parameters
        ----------
        file_name : str
          Something like subject.json

        Returns
        -------
        True if self.settings.metadata_dir is not None and file is in that dir

        """
        if self.settings.metadata_dir is None:
            return False
        file_path_to_check = os.path.join(self.settings.metadata_dir, file_name)
        if os.path.isfile(file_path_to_check):
            return True
        else:
            return False

    def _get_file_from_user_defined_directory(self, file_name: str) -> dict:
        """
        Get a file from a user defined directory
        Parameters
        ----------
        file_name : str
          Like subject.json

        Returns
        -------
        File contents as a dictionary

        """
        if self.settings.metadata_dir is None:
            return None
        file_path = os.path.join(self.settings.metadata_dir, file_name)
        with open(file_path, "r") as f:
            contents = json.load(f)
        return contents

    def _get_prefixed_files_from_user_defined_directory(self, file_name_prefix: str) -> list[dict]:
        """
        Get all files with a given prefix from a user defined directory

        Parameters
        ----------
        file_name_prefix : str
            Prefix, e.g. "instrument"

        Returns
        -------
        list[dict]
            File contents as a list of dictionaries
        """
        if self.settings.metadata_dir is None:
            return []
        return self._get_prefixed_files_from_directory(self.settings.metadata_dir, file_name_prefix)

    def _get_prefixed_files_from_directory(self, directory: str, file_name_prefix: str) -> list[dict]:
        """
        Get all files with a given prefix from a specified directory

        Parameters
        ----------
        directory : str
            Directory to search in
        file_name_prefix : str
            Prefix, e.g. "instrument"

        Returns
        -------
        list[dict]
            File contents as a list of dictionaries
        """
        if not os.path.exists(directory):
            return []

        file_paths = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.startswith(file_name_prefix) and f.endswith(".json")
        ]
        file_names = [os.path.basename(f) for f in file_paths]
        logging.info(f"Found {len(file_paths)} {file_name_prefix} files: {file_names}")
        contents = []
        for file_path in file_paths:
            with open(file_path, "r") as f:
                contents.append(json.load(f))
        return contents

    def get_funding(self) -> list:
        """Get funding metadata from the V2 endpoint

        Returns
        -------
        list
            A list of funding sources
        """
        if not self.settings.project_name:
            return []

        funding_url = f"{self.settings.metadata_service_url}" f"/api/v2/funding/{self.settings.project_name}"
        funding_info = metadata_service_helper(funding_url)
        return funding_info if funding_info else []

    def get_investigators(self) -> list:
        """Get investigators metadata from the V2 endpoint

        Returns
        -------
        list
            A list of investigators
        """
        if not self.settings.project_name:
            return []

        investigators_url = (
            f"{self.settings.metadata_service_url}" f"/api/v2/investigators/{self.settings.project_name}"
        )
        investigators_info = metadata_service_helper(investigators_url)
        if investigators_info is None:
            return []
        # Deduplicate investigators by name and sort
        seen_names = set()
        unique_investigators = []
        for investigator in investigators_info:
            name = investigator.get("name", "")
            if name and name not in seen_names:
                seen_names.add(name)
                unique_investigators.append(investigator)
        unique_investigators.sort(key=lambda x: x.get("name", ""))
        return unique_investigators

    def build_data_description(self, acquisition_start_time: str, subject_id: str) -> dict:
        """Build data description metadata"""
        logging.info("Gathering data description metadata.")
        file_name = DataDescription.default_filename()

        # Check if file already exists in user directory
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)

        acquisition_start_time = normalize_utc_timezone(acquisition_start_time)  # remove when we're past Python 3.11
        creation_time = datetime.fromisoformat(acquisition_start_time)
        logging.info(f"Using acquisition start time: {creation_time}")

        # Get funding information
        funding_source = self.get_funding()
        investigators = self.get_investigators()

        # Get modalities
        modalities = self.settings.modalities

        # Create new data description
        new_data_description = DataDescription(
            creation_time=creation_time,
            institution=Organization.AIND,
            project_name=self.settings.project_name,
            modalities=modalities,
            funding_source=funding_source,
            investigators=investigators,
            data_level=DataLevel.RAW,
            subject_id=subject_id,
            tags=self.settings.tags,
            group=self.settings.group,
            restrictions=self.settings.restrictions,
            data_summary=self.settings.data_summary,
        )

        # Over-write creation_time now that the .name field has been populated
        new_data_description.creation_time = datetime.now(tz=timezone.utc)

        return json.loads(new_data_description.model_dump_json())

    def get_subject(self, subject_id: Optional[str] = None) -> Optional[dict]:
        """Get subject metadata

        Parameters
        ----------
        subject_id : Optional[str]
            Subject ID to use. If None, uses the subject_id from settings.
        """
        logging.info("Gathering subject metadata.")
        file_name = Subject.default_filename()
        if subject_id is None:
            subject_id = self.settings.subject_id

        if not subject_id:
            logging.warning("No subject_id provided.")
            return None

        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(
                f"No subject file found in directory. Downloading "
                f"{self.settings.subject_id} from "
                f"{self.settings.metadata_service_url}"
            )
            base_url = urljoin(
                self.settings.metadata_service_url,
                self.settings.metadata_service_subject_endpoint,
            )
            contents = get_subject(subject_id, base_url=base_url)
        else:
            logging.debug(f"Using existing {file_name}.")
            contents = self._get_file_from_user_defined_directory(file_name=file_name)
        return contents

    def get_procedures(self, subject_id: str) -> Optional[dict]:
        """Get procedures metadata

        Parameters
        ----------
        subject_id : str
            Subject ID to use for fetching procedures.
        """
        logging.info("Gathering procedures metadata.")
        file_name = Procedures.default_filename()

        if not subject_id:
            logging.warning("No subject_id provided.")
            return None

        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(
                f"No procedures file found in directory. Downloading "
                f"{self.settings.subject_id} from "
                f"{self.settings.metadata_service_url}"
            )
            base_url = urljoin(
                self.settings.metadata_service_url,
                self.settings.metadata_service_procedures_endpoint,
            )
            contents = get_procedures(subject_id, base_url=base_url)
        else:
            logging.debug(f"Using existing {file_name}.")
            contents = self._get_file_from_user_defined_directory(file_name=file_name)
        return contents

    def _run_mappers_for_acquisition(self):
        """
        Run mappers for any files in metadata_dir matching a registry key.
        For each file named <mapper>.json, run the corresponding mapper and output acquisition_<mapper>.json.
        """
        if self.settings.metadata_dir is None:
            return
        input_dir = self.settings.metadata_dir
        output_dir = self.settings.output_dir
        # For each registry key, check if <key>.json exists
        for mapper_name in registry.keys():
            input_filename = f"{mapper_name}.json"
            input_path = os.path.join(input_dir, input_filename)
            output_filename = f"acquisition_{mapper_name}.json"
            output_path = os.path.join(output_dir, output_filename)
            # Only run if input exists and output does not already exist
            if os.path.isfile(input_path) and not os.path.isfile(output_path):
                mapper_cls = registry[mapper_name]
                # Create job settings for the mapper
                job_settings = MapperJobSettings(input_filepath=Path(input_path), output_filepath=Path(output_path))
                try:
                    mapper = mapper_cls()
                    mapper.run_job(job_settings)
                    logging.info(f"Ran mapper '{mapper_name}' for {input_filename} -> {output_filename}")
                except Exception as e:
                    logging.error(f"Error running mapper '{mapper_name}': {e}")
                    if self.settings.raise_if_mapper_errors:
                        raise e

    def get_acquisition(self) -> Optional[dict]:
        """Get acquisition metadata"""
        logging.info("Gathering acquisition metadata.")
        file_name = Acquisition.default_filename()

        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)
        else:
            # Run mappers for any matching files
            self._run_mappers_for_acquisition()
            # then gather all acquisition files with prefixes from output directory
            files = self._get_prefixed_files_from_directory(
                directory=self.settings.output_dir, file_name_prefix="acquisition"
            )
            if files:
                return self._merge_models(Acquisition, files)
            else:
                logging.debug("No acquisition metadata file found.")
                return None

    def _merge_models(self, model_class, models: list[dict]) -> dict:
        """Merge multiple metadata dictionaries into one."""
        logging.info(f"Merging {len(models)} {model_class.__name__} models.")
        model_objs = [model_class.model_validate(model) for model in models]

        merged_model = model_objs[0]
        for model in model_objs[1:]:
            merged_model = merged_model + model

        return merged_model.model_dump(mode="json")

    def get_instrument(self) -> Optional[dict]:
        """Get instrument metadata"""
        logging.info("Gathering instrument metadata.")
        file_name = Instrument.default_filename()

        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)
        else:
            files = self._get_prefixed_files_from_user_defined_directory(file_name_prefix="instrument")
            if files:
                return self._merge_models(Instrument, files)
            else:
                logging.debug("No instrument metadata file found.")
                return None

    def get_processing(self) -> Optional[dict]:
        """Get processing metadata"""
        logging.info("Gathering processing metadata.")
        file_name = Processing.default_filename()

        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)
        else:
            logging.debug("No processing metadata file found.")
            return None

    def get_quality_control(self) -> Optional[dict]:
        """Get quality control metadata"""
        logging.info("Gathering quality control metadata.")
        file_name = QualityControl.default_filename()

        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)
        else:
            files = self._get_prefixed_files_from_user_defined_directory(file_name_prefix="quality_control")
            if files:
                return self._merge_models(QualityControl, files)
            else:
                logging.debug("No quality control metadata file found.")
                return None

    def get_model(self) -> Optional[dict]:
        """Get model metadata"""
        logging.info("Gathering model metadata.")
        file_name = Model.default_filename()

        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)
        else:
            logging.debug("No model metadata file found.")
            return None

    def _write_json_file(self, filename: str, contents: dict) -> None:
        """
        Write a json file to the output directory
        Parameters
        ----------
        filename : str
          Name of the file to write to (e.g., subject.json)
        contents : dict
          Contents to write to the json file
        """
        output_file = os.path.join(self.settings.output_dir, filename)
        with open(output_file, "w") as f:
            json.dump(contents, f, indent=3, ensure_ascii=False, sort_keys=True)

    def _construct_metadata(self, core_metadata: Dict[str, Any]) -> Metadata:
        """
        Construct Metadata object from core metadata dictionary

        Parameters
        ----------
        core_metadata : Dict[str, Any]
            Dictionary containing all core metadata

        Returns
        -------
        Metadata
            The constructed metadata object
        """
        name = core_metadata["data_description"]["name"]
        metadata = Metadata(
            subject=core_metadata.get("subject"),
            data_description=core_metadata.get("data_description"),
            procedures=core_metadata.get("procedures"),
            acquisition=core_metadata.get("acquisition"),
            instrument=core_metadata.get("instrument"),
            processing=core_metadata.get("processing"),
            quality_control=core_metadata.get("quality_control"),
            model=core_metadata.get("model"),
            name=name,
            location="",  # we're only doing validation here, no need to set real location
        )
        return metadata

    def validate_and_create_metadata(self, core_metadata: Dict[str, Any]) -> Metadata | dict:
        """
        Validate core metadata and create Metadata object

        Parameters
        ----------
        core_metadata : Dict[str, Any]
            Dictionary containing all core metadata

        Returns
        -------
        Metadata
            The constructed metadata object
        """
        logging.info("Validating and creating metadata object")

        name = core_metadata["data_description"]["name"]

        if self.settings.raise_if_invalid:
            return self._construct_metadata(core_metadata)
        else:
            # Try to create a valid Metadata object first
            try:
                metadata = self._construct_metadata(core_metadata)
                logging.info("Metadata validation successful!")
                return metadata

            except ValidationError as e:
                logging.warning(f"Metadata validation failed: {e}")
                logging.info("Creating metadata object with validation bypass")

                # Display validation errors to user
                logging.warning("Validation Errors Found:")
                for error in e.errors():
                    logging.warning(f"  - {error['loc']}: {error['msg']}")

                # Use create_metadata_json to construct metadata object
                metadata = create_metadata_json(
                    core_jsons={
                        "subject": core_metadata.get("subject"),
                        "data_description": core_metadata.get("data_description"),
                        "procedures": core_metadata.get("procedures"),
                        "acquisition": core_metadata.get("acquisition"),
                        "instrument": core_metadata.get("instrument"),
                        "processing": core_metadata.get("processing"),
                        "quality_control": core_metadata.get("quality_control"),
                        "model": core_metadata.get("model"),
                    },
                    name=name,
                    location="",
                )
                return metadata

    def _validate_acquisition_start_time(self, acquisition_start_time: str) -> str:
        """
        Validate that acquisition_start_time matches settings if both are provided.

        Parameters
        ----------
        acquisition_start_time : str
            The acquisition start time from acquisition metadata

        Returns
        -------
        str
            The validated acquisition_start_time

        Raises
        ------
        ValueError
            If acquisition_start_time doesn't match settings and raise_if_invalid is True
        """
        acquisition_start_time = normalize_utc_timezone(acquisition_start_time)  # remove when we're past Python 3.11
        local_acq_start_time = datetime.fromisoformat(acquisition_start_time)

        if self.settings.acquisition_start_time and local_acq_start_time != self.settings.acquisition_start_time:
            error_msg = (
                "acquisition_start_time from acquisition metadata does not match "
                "the acquisition_start_time provided in settings."
            )
            if self.settings.raise_if_invalid:
                raise ValueError(error_msg)
            else:
                logging.error(error_msg)

        return acquisition_start_time

    def _validate_and_get_subject_id(self, acquisition: Optional[dict]) -> str:
        """
        Get and validate subject_id from acquisition or settings.

        Parameters
        ----------
        acquisition : Optional[dict]
            The acquisition metadata dictionary

        Returns
        -------
        str
            The validated subject_id

        Raises
        ------
        ValueError
            If subject_id is missing or doesn't match between acquisition and settings
        """
        subject_id = acquisition.get("subject_id") if acquisition else None

        if not subject_id:
            if self.settings.subject_id:
                subject_id = self.settings.subject_id
                logging.info(
                    f"No subject_id found in acquisition metadata. " f"Using provided subject_id: {subject_id}"
                )
            else:
                raise ValueError(
                    "subject_id is required but not provided. "
                    "Either provide acquisition.json with subject_id, "
                    "or provide subject_id in the settings."
                )
        else:
            # Validate that subject_id matches if both are provided
            if self.settings.subject_id and subject_id != self.settings.subject_id:
                error_msg = (
                    f"subject_id from acquisition metadata ({subject_id}) does not match "
                    f"the subject_id provided in settings ({self.settings.subject_id})."
                )
                if self.settings.raise_if_invalid:
                    raise ValueError(error_msg)
                else:
                    logging.error(error_msg)

        return subject_id

    def add_core_metadata(self, core_metadata: dict, subject_id: str) -> Dict[str, Any]:
        """Get all core metadata as a dictionary

        Parameters
        ----------
        core_metadata : dict
            Dictionary to add metadata to
        subject_id : Optional[str]
            Subject ID to use for fetching subject and procedures. If None, uses settings.
        """

        subject = self.get_subject(subject_id=subject_id)
        if subject:
            core_metadata["subject"] = subject
            self._write_json_file(Subject.default_filename(), subject)

        procedures = self.get_procedures(subject_id=subject_id)
        if procedures:
            core_metadata["procedures"] = procedures
            self._write_json_file(Procedures.default_filename(), procedures)

        instrument = self.get_instrument()
        if instrument:
            core_metadata["instrument"] = instrument
            self._write_json_file(Instrument.default_filename(), instrument)

        processing = self.get_processing()
        if processing:
            core_metadata["processing"] = processing
            self._write_json_file(Processing.default_filename(), processing)

        quality_control = self.get_quality_control()
        if quality_control:
            core_metadata["quality_control"] = quality_control
            self._write_json_file(QualityControl.default_filename(), quality_control)

        model = self.get_model()
        if model:
            core_metadata["model"] = model
            self._write_json_file(Model.default_filename(), model)

        return core_metadata

    def run_job(self) -> None:
        """Run job"""
        logging.info("Starting run_job")

        # Gather all core metadata
        core_metadata = {}

        # Get acquisition first so that we can use the acquisition_start_time
        # for the data_description
        acquisition = self.get_acquisition()
        if acquisition:
            core_metadata["acquisition"] = acquisition
            self._write_json_file(Acquisition.default_filename(), acquisition)

        # Get and validate acquisition_start_time
        acquisition_start_time = acquisition.get("acquisition_start_time") if acquisition else None
        if not acquisition_start_time:
            if self.settings.acquisition_start_time:
                acquisition_start_time = self.settings.acquisition_start_time.isoformat()
                logging.info(
                    f"No acquisition_start_time found in acquisition metadata. "
                    f"Using provided acquisition_start_time: {acquisition_start_time}"
                )
            else:
                raise ValueError(
                    "acquisition_start_time is required but not provided. "
                    "Either provide acquisition.json with acquisition_start_time, "
                    "or provide acquisition_start_time in the settings."
                )

        # Validate acquisition_start_time matches settings if both are provided
        acquisition_start_time = self._validate_acquisition_start_time(acquisition_start_time)

        # Get and validate subject_id
        subject_id = self._validate_and_get_subject_id(acquisition)

        # Always create data description (required)
        data_description = self.build_data_description(
            acquisition_start_time=acquisition_start_time,
            subject_id=subject_id,
        )
        if data_description:
            core_metadata["data_description"] = data_description
            self._write_json_file(DataDescription.default_filename(), data_description)

        # Get other metadata (optional)
        # Adds subject, procedures, instrument, processing, quality_control, model, if available
        core_metadata = self.add_core_metadata(core_metadata=core_metadata, subject_id=subject_id)

        self.validate_and_create_metadata(core_metadata)

        logging.info("Finished job.")


if __name__ == "__main__":
    # Allows a user to input job-settings as json string in command line
    if len(sys.argv[1:]) == 2 and sys.argv[1] == "--job-settings":
        main_job_settings = JobSettings.model_validate_json(sys.argv[2])
    else:
        main_job_settings = JobSettings()
    job = GatherMetadataJob(settings=main_job_settings)
    job.run_job()
