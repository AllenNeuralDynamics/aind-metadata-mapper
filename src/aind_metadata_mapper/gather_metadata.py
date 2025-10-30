"""Module to gather metadata from different sources."""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
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

from aind_metadata_mapper.models import JobSettings
from aind_metadata_mapper.base import MapperJobSettings
from aind_metadata_mapper.mapper_registry import registry


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
        if not os.path.exists(self.settings.metadata_dir):
            return []

        file_paths = [
            os.path.join(self.settings.metadata_dir, f)
            for f in os.listdir(self.settings.metadata_dir)
            if f.startswith(file_name_prefix) and f.endswith(".json")
        ]
        file_names = [os.path.basename(f) for f in file_paths]
        logging.info(f"Found {len(file_paths)} {file_name_prefix} files: {file_names}")
        contents = []
        for file_path in file_paths:
            with open(file_path, "r") as f:
                contents.append(json.load(f))
        return contents

    def get_funding(self) -> tuple[list, list]:
        """Get funding and investigators metadata from the V2 endpoint

        Returns
        -------
        tuple[list, list]
            A tuple of (funding_source, investigators)
        """
        if not self.settings.project_name:
            return [], []

        try:
            funding_url = f"{self.settings.metadata_service_url}" f"/api/v2/funding/{self.settings.project_name}"
            response = requests.get(funding_url)
            if response.status_code == 200:
                funding_info = response.json()
            else:
                logging.warning(f"Unable to retrieve funding info: {response.status_code}")
                return [], []
        except Exception as e:
            logging.warning(f"Error retrieving funding info: {e}")
            return [], []

        investigators = []
        parsed_funding_info = []

        for f in funding_info:
            project_investigators = f.get("investigators", [])
            investigators.extend(project_investigators)

            funding_info_without_investigators = {k: v for k, v in f.items() if k != "investigators"}
            parsed_funding_info.append(funding_info_without_investigators)

        # Deduplicate investigators by name and sort
        seen_names = set()
        unique_investigators = []
        for investigator in investigators:
            name = investigator.get("name", "")
            if name and name not in seen_names:
                seen_names.add(name)
                unique_investigators.append(investigator)

        unique_investigators.sort(key=lambda x: x.get("name", ""))
        investigators_list = unique_investigators

        return parsed_funding_info, investigators_list

    def build_data_description(self, acquisition_start_time: Optional[str]) -> dict:
        """Build data description metadata"""
        logging.info("Gathering data description metadata.")
        file_name = DataDescription.default_filename()

        # Check if file already exists in user directory
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)

        if acquisition_start_time:
            acquisition_start_time.replace("Z", "+00:00")  # remove when we're past Python 3.11
            creation_time = datetime.fromisoformat(acquisition_start_time)
            logging.info(f"Using acquisition start time: {creation_time}")
        else:
            creation_time = datetime.now()
            logging.info(f"No start time available, using creation time: {creation_time}")

        # Get funding information
        funding_source, investigators = self.get_funding()

        # Create new data description
        new_data_description = DataDescription(
            creation_time=creation_time,
            institution=Organization.AIND,
            project_name=self.settings.project_name,
            modalities=self.settings.modalities,
            funding_source=funding_source,
            investigators=investigators,
            data_level=DataLevel.RAW,
            subject_id=self.settings.subject_id,
            tags=self.settings.tags,
            group=self.settings.group,
            restrictions=self.settings.restrictions,
            data_summary=self.settings.data_summary,
        )

        # Over-write creation_time now that the .name field has been populated
        new_data_description.creation_time = datetime.now(tz=timezone.utc)

        return json.loads(new_data_description.model_dump_json())

    def get_subject(self) -> Optional[dict]:
        """Get subject metadata"""
        logging.info("Gathering subject metadata.")
        file_name = Subject.default_filename()
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
            try:
                response = requests.get(f"{self.settings.metadata_service_url}" f"/api/v2/subject/{subject_id}")
                if response.status_code == 200:
                    contents = response.json().get("data", response.json())
                else:
                    logging.error(f"Subject {subject_id} not found in service (status: {response.status_code})")
                    contents = None
            except Exception as e:
                logging.error(f"Failed to retrieve subject metadata: {e}")
                contents = None
        else:
            logging.debug(f"Using existing {file_name}.")
            contents = self._get_file_from_user_defined_directory(file_name=file_name)
        return contents

    def get_procedures(self) -> Optional[dict]:
        """Get procedures metadata"""
        logging.info("Gathering procedures metadata.")
        file_name = Procedures.default_filename()
        subject_id = self.settings.subject_id

        if not subject_id:
            logging.warning("No subject_id provided for procedures.")
            return None

        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(
                f"No procedures file found in directory. Downloading "
                f"{self.settings.subject_id} from "
                f"{self.settings.metadata_service_url}"
            )
            try:
                response = requests.get(f"{self.settings.metadata_service_url}" f"/api/v2/procedures/{subject_id}")
                if response.status_code == 200:
                    contents = response.json().get("data", response.json())
                else:
                    logging.error(f"Procedures for {subject_id} not found in service (status: {response.status_code})")
                    contents = None
            except Exception as e:
                logging.error(f"Failed to retrieve procedures metadata: {e}")
                contents = None
        else:
            logging.debug(f"Using existing {file_name}.")
            contents = self._get_file_from_user_defined_directory(file_name=file_name)
        return contents

    def _run_mappers_for_acquisition(self):
        """
        Run mappers for any files in metadata_dir matching a registry key.
        For each file named <mapper>.json, run the corresponding mapper and output acquisition_<mapper>.json.
        """
        metadata_dir = self.settings.metadata_dir
        # For each registry key, check if <key>.json exists
        for mapper_name in registry.keys():
            input_filename = f"{mapper_name}.json"
            input_path = os.path.join(metadata_dir, input_filename)
            output_filename = f"acquisition_{mapper_name}.json"
            output_path = os.path.join(metadata_dir, output_filename)
            # Only run if input exists and output does not already exist
            if os.path.isfile(input_path) and not os.path.isfile(output_path):
                mapper_cls = registry[mapper_name]
                # Create job settings for the mapper
                job_settings = MapperJobSettings(input_filepath=input_path, output_filepath=output_path)
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
            # then gather all acquisition files with prefixes
            files = self._get_prefixed_files_from_user_defined_directory(file_name_prefix="acquisition")
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

        return merged_model.model_dump()

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
        Write a json file
        Parameters
        ----------
        filename : str
          Name of the file to write to (e.g., subject.json)
        contents : dict
          Contents to write to the json file
        """
        output_file = os.path.join(self.settings.metadata_dir, filename)
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
            location=self.settings.location if self.settings.location else "",
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
                    location=self.settings.location if self.settings.location else "",
                )
                return metadata

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

        # Always create data description (required)
        data_description = self.build_data_description(
            acquisition.get("acquisition_start_time") if acquisition else None
        )
        if data_description:
            core_metadata["data_description"] = data_description
            self._write_json_file(DataDescription.default_filename(), data_description)

        # Get other metadata (optional)
        subject = self.get_subject()
        if subject:
            core_metadata["subject"] = subject
            self._write_json_file(Subject.default_filename(), subject)

        procedures = self.get_procedures()
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

        self.validate_and_create_metadata(core_metadata)

        logging.info("Finished job.")


if __name__ == "__main__":
    main_job_settings = JobSettings(metadata_dir="./metadata")
    job = GatherMetadataJob(settings=main_job_settings)
    job.run_job()
