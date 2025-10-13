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

    def build_data_description(self) -> dict:
        """Build data description metadata"""
        logging.info("Gathering data description metadata.")
        file_name = DataDescription.default_filename()

        # Check if file already exists in user directory
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)

        # Get acquisition data to extract start time
        acquisition_data = self.get_acquisition()
        creation_time = datetime.now()

        if acquisition_data and "acquisition_start_time" in acquisition_data:
            start_time_str = acquisition_data["acquisition_start_time"]
            iso_time_str = start_time_str.replace("Z", "+00:00")
            creation_time = datetime.fromisoformat(iso_time_str)
            logging.info(f"Using acquisition start time: {creation_time}")

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

    def get_acquisition(self) -> Optional[dict]:
        """Get acquisition metadata"""
        logging.info("Gathering acquisition metadata.")
        file_name = Acquisition.default_filename()

        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)
        else:
            logging.debug("No acquisition metadata file found.")
            return None

    def get_instrument(self) -> Optional[dict]:
        """Get instrument metadata"""
        logging.info("Gathering instrument metadata.")
        file_name = Instrument.default_filename()

        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(f"Using existing {file_name}.")
            return self._get_file_from_user_defined_directory(file_name=file_name)
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

    def validate_and_create_metadata(self, core_metadata: Dict[str, Any]) -> Metadata:
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
                print("Validation Errors Found:")
                for error in e.errors():
                    print(f"  - {error['loc']}: {error['msg']}")
                print()

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

        # Always create data description (required)
        data_description = self.build_data_description()
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

        acquisition = self.get_acquisition()
        if acquisition:
            core_metadata["acquisition"] = acquisition
            self._write_json_file(Acquisition.default_filename(), acquisition)

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
