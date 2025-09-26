"""Module to gather metadata from different sources for AIND data schema 2.0"""

import json
import logging
from inspect import signature
from pathlib import Path
from typing import List, Optional, Type

import requests
from aind_data_schema.base import DataCoreModel
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import Processing
from aind_data_schema.core.quality_control import QualityControl
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.pid_names import PIDName
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from aind_metadata_mapper.models import JobSettings


class GatherMetadataJob:
    """Class to handle retrieving metadata for AIND data schema 2.0"""

    def __init__(self, settings: JobSettings):
        """
        Class constructor
        Parameters
        ----------
        settings : JobSettings
        """
        self.settings = settings
        self.validation_errors: List[str] = []
        
        # Convert metadata_dir to Path object if it's a string
        if isinstance(self.settings.metadata_dir, str):
            self.settings.metadata_dir = Path(self.settings.metadata_dir)

    def _does_file_exist_in_user_defined_dir(self, file_name: str) -> bool:
        """
        Check whether a file exists in a directory.
        
        Parameters
        ----------
        file_name : str
            Something like subject.json

        Returns
        -------
        bool
            True if self.settings.metadata_dir is not None and file is in
            that dir
        """
        if (
            self.settings.metadata_dir is not None and
            isinstance(self.settings.metadata_dir, Path)
        ):
            file_path_to_check = self.settings.metadata_dir / file_name
            return file_path_to_check.is_file()
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
        dict
            File contents as a dictionary
        """
        if (
            self.settings.metadata_dir is None or
            not isinstance(self.settings.metadata_dir, Path)
        ):
            return {}
            
        file_path = self.settings.metadata_dir / file_name
        try:
            with open(file_path, "r") as f:
                contents = json.load(f)
            return contents
        except Exception as e:
            error_msg = f"Failed to load {file_name} from user directory: {e}"
            logging.error(error_msg)
            self.validation_errors.append(error_msg)
            return {}

    def get_subject(self, service_session: requests.Session) -> dict:
        """Get subject metadata"""
        if self.settings.subject_settings is None:
            raise ValueError("Subject settings must be provided")
            
        file_name = Subject.default_filename()
        
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            try:
                if self.settings.metadata_service_domain is None:
                    raise ValueError(
                        "Metadata service domain must be provided"
                    )
                    
                response = service_session.get(
                    f"{self.settings.metadata_service_domain}/"
                    f"{self.settings.subject_settings.metadata_service_path}/"
                    f"{self.settings.subject_settings.subject_id}"
                )

                if response.status_code < 300 or response.status_code == 406:
                    json_content = response.json()
                    return json_content["data"]
                else:
                    error_msg = (
                        f"Subject metadata request failed: {response.json()}"
                    )
                    logging.error(error_msg)
                    self.validation_errors.append(error_msg)
                    return {}
            except Exception as e:
                error_msg = f"Failed to retrieve subject metadata: {e}"
                logging.error(error_msg)
                self.validation_errors.append(error_msg)
                return {}
        else:
            return self._get_file_from_user_defined_directory(
                file_name=file_name
            )

    def get_procedures(
        self, service_session: requests.Session
    ) -> Optional[dict]:
        """Get procedures metadata"""
        if self.settings.procedures_settings is None:
            return None
            
        file_name = Procedures.default_filename()
        
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            try:
                if self.settings.metadata_service_domain is None:
                    raise ValueError(
                        "Metadata service domain must be provided"
                    )
                    
                procedures_file_path = (
                    self.settings.procedures_settings.metadata_service_path
                )
                response = service_session.get(
                    f"{self.settings.metadata_service_domain}/"
                    f"{procedures_file_path}/"
                    f"{self.settings.procedures_settings.subject_id}"
                )

                if response.status_code < 300 or response.status_code == 406:
                    json_content = response.json()
                    return json_content["data"]
                else:
                    response_data = response.json()
                    error_msg = (
                        f"Procedures metadata request failed: {response_data}"
                    )
                    logging.error(error_msg)
                    self.validation_errors.append(error_msg)
                    return None
            except Exception as e:
                error_msg = f"Failed to retrieve procedures metadata: {e}"
                logging.error(error_msg)
                self.validation_errors.append(error_msg)
                return None
        else:
            return self._get_file_from_user_defined_directory(
                file_name=file_name
            )

    def _get_funding_info(
        self, service_session: requests.Session, domain: str,
        url_path: str, project_name: str
    ):
        """Utility method to retrieve funding info from metadata service"""
        try:
            url = "/".join([domain, url_path, project_name])
            response = service_session.get(url)
            
            if response.status_code == 200:
                funding_info = [response.json().get("data")]
            elif response.status_code == 300:
                funding_info = response.json().get("data")
            else:
                funding_info = []
                
            investigators = set()
            parsed_funding_info = []
            
            for f in funding_info:
                project_investigators = (
                    "" if f.get("investigators", None) is None
                    else f.get("investigators", "").split(",")
                )
                investigators_pid_names = [
                    PIDName(name=p.strip()).model_dump_json()
                    for p in project_investigators if p.strip()
                ]
                investigators.update(investigators_pid_names)
                
                funding_info_without_investigators = {
                    k: v for k, v in f.items() if k != "investigators"
                }
                parsed_funding_info.append(funding_info_without_investigators)
                
            investigators = [
                PIDName.model_validate_json(i) for i in investigators
            ]
            investigators.sort(key=lambda x: x.name)
            return parsed_funding_info, investigators
            
        except Exception as e:
            error_msg = f"Failed to retrieve funding info: {e}"
            logging.error(error_msg)
            self.validation_errors.append(error_msg)
            return [], []

    def get_data_description(
        self, service_session: requests.Session
    ) -> dict:
        """Get data description metadata"""
        file_name = DataDescription.default_filename()
        
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            return self._create_data_description_from_settings(service_session)
        else:
            return self._get_file_from_user_defined_directory(
                file_name=file_name
            )

    def _create_data_description_from_settings(
        self, service_session: requests.Session
    ) -> dict:
        """Create data description from settings"""
        if self.settings.data_description_settings is None:
            raise ValueError("Data description settings must be provided")
            
        try:
            # Parse basic settings from name
            basic_settings = DataDescription.parse_name(
                name=self.settings.data_description_settings.name
            )
            
            ds_settings = self.settings.data_description_settings
            project_name = ds_settings.project_name
            
            if self.settings.metadata_service_domain is None:
                raise ValueError("Metadata service domain must be provided")
            
            funding_source, investigator_list = self._get_funding_info(
                service_session,
                self.settings.metadata_service_domain,
                ds_settings.metadata_service_path_funding,
                project_name,
            )

            institution = ds_settings.institution
            modality = ds_settings.modality

            data_description_dict = {
                **basic_settings,
                "modality": modality,
                "institution": institution,
                "funding_source": funding_source,
                "investigators": investigator_list,
            }
            
            return data_description_dict
            
        except ValidationError as e:
            error_msg = f"Data description validation error: {e}"
            logging.error(error_msg)
            self.validation_errors.append(error_msg)
            return {}
        except Exception as e:
            error_msg = f"Failed to create data description: {e}"
            logging.error(error_msg)
            self.validation_errors.append(error_msg)
            return {}

    def get_processing_metadata(self) -> Optional[dict]:
        """Get processing metadata"""
        file_name = Processing.default_filename()
        
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            if self.settings.processing_settings is not None:
                try:
                    # For now, return the pipeline process as provided
                    # This will be validated downstream when creating metadata
                    return self.settings.processing_settings.pipeline_process
                except Exception as e:
                    error_msg = f"Failed to create processing metadata: {e}"
                    logging.error(error_msg)
                    self.validation_errors.append(error_msg)
                    return None
            else:
                return None
        else:
            return self._get_file_from_user_defined_directory(
                file_name=file_name
            )

    def get_acquisition_metadata(self) -> Optional[dict]:
        """Get acquisition metadata from provided file path"""
        file_name = Acquisition.default_filename()
        
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            if self.settings.acquisition_settings is not None:
                try:
                    acquisition_path = Path(
                        self.settings.acquisition_settings.acquisition_path
                    )
                    if acquisition_path.is_file():
                        with open(acquisition_path, "r") as f:
                            return json.load(f)
                    else:
                        error_msg = (
                            f"Acquisition file not found: {acquisition_path}"
                        )
                        logging.error(error_msg)
                        self.validation_errors.append(error_msg)
                        return None
                except Exception as e:
                    error_msg = f"Failed to load acquisition metadata: {e}"
                    logging.error(error_msg)
                    self.validation_errors.append(error_msg)
                    return None
            else:
                return None
        else:
            return self._get_file_from_user_defined_directory(
                file_name=file_name
            )

    def get_instrument_metadata(
        self, service_session: requests.Session
    ) -> Optional[dict]:
        """Get instrument metadata from service or file"""
        file_name = Instrument.default_filename()
        
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            if self.settings.instrument_settings is not None:
                try:
                    instrument_file_path = (
                        self.settings.instrument_settings.metadata_service_path
                    )
                    response = service_session.get(
                        f"{self.settings.metadata_service_domain}/"
                        f"{instrument_file_path}/"
                        f"{self.settings.instrument_settings.instrument_id}"
                    )
                    
                    if (
                        response.status_code < 300 or
                        response.status_code == 422
                    ):
                        json_content = response.json()
                        return json_content.get("data", json_content)
                    else:
                        response_data = response.json()
                        error_msg = (
                            "Instrument metadata request failed: "
                            f"{response_data}"
                        )
                        logging.error(error_msg)
                        self.validation_errors.append(error_msg)
                        return None
                except Exception as e:
                    error_msg = f"Failed to retrieve instrument metadata: {e}"
                    logging.error(error_msg)
                    self.validation_errors.append(error_msg)
                    return None
            else:
                return None
        else:
            return self._get_file_from_user_defined_directory(
                file_name=file_name
            )

    def get_quality_control_metadata(self) -> Optional[dict]:
        """Get quality_control metadata"""
        file_name = QualityControl.default_filename()
        
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            return self._get_file_from_user_defined_directory(
                file_name=file_name
            )
        else:
            return None

    def _validate_and_load_model(
        self, filepath: Optional[Path], model: Type[DataCoreModel]
    ) -> Optional[dict]:
        """
        Validates contents of file with an AindCoreModel
        
        Parameters
        ----------
        filepath : Optional[Path]
        model : Type[AindCoreModel]

        Returns
        -------
        Optional[dict]
        """
        if filepath is not None and filepath.is_file():
            try:
                with open(filepath, "r") as f:
                    contents = json.load(f)
                # Validate the model
                model.model_validate(contents)
                return contents
            except ValidationError as e:
                error_msg = f"Validation error in {filepath.name}: {e}"
                logging.error(error_msg)
                self.validation_errors.append(error_msg)
                return contents  # Return invalid data for downstream handling
            except Exception as e:
                error_msg = f"Failed to load {filepath.name}: {e}"
                logging.error(error_msg)
                self.validation_errors.append(error_msg)
                return None
        else:
            return None

    def get_main_metadata(self) -> dict:
        """Get serialized main Metadata model"""
        
        if self.settings.metadata_settings is None:
            raise ValueError("Metadata settings must be provided")
            
        subject = self._validate_and_load_model(
            self.settings.metadata_settings.subject_filepath, Subject
        )
        data_description = self._validate_and_load_model(
            self.settings.metadata_settings.data_description_filepath,
            DataDescription,
        )
        procedures = self._validate_and_load_model(
            self.settings.metadata_settings.procedures_filepath, Procedures
        )
        quality_control = self._validate_and_load_model(
            self.settings.metadata_settings.quality_control_filepath,
            QualityControl,
        )
        acquisition = self._validate_and_load_model(
            self.settings.metadata_settings.acquisition_filepath, Acquisition
        )
        instrument = self._validate_and_load_model(
            self.settings.metadata_settings.instrument_filepath, Instrument
        )
        processing = self._validate_and_load_model(
            self.settings.metadata_settings.processing_filepath, Processing
        )
        
        # For schema 2.0, we'll create a basic metadata structure
        # This avoids complex validation during construction
        metadata_json = {
            "name": self.settings.metadata_settings.name,
            "location": "",  # Leave empty for downstream filling
            "subject": subject,
            "data_description": data_description,
            "procedures": procedures,
            "processing": processing,
            "acquisition": acquisition,
            "instrument": instrument,
            "quality_control": quality_control,
        }
        
        # Try to validate by constructing a Metadata object
        try:
            # This is just for validation - we don't use the result
            Metadata.model_validate(metadata_json)
        except Exception as e:
            error_msg = f"Metadata validation warning: {e}"
            logging.warning(error_msg)
            self.validation_errors.append(error_msg)
            
        return metadata_json

    def _write_json_file(self, filename: str, contents: dict) -> None:
        """
        Write a json file
        
        Parameters
        ----------
        filename : str
            Name of the file to write to (e.g., subject.json)
        contents : dict
            Contents to write to the json file

        Returns
        -------
        None
        """
        output_path = self.settings.directory_to_write_to / filename
        try:
            with open(output_path, "w") as f:
                json.dump(contents, f, indent=3)
        except Exception as e:
            error_msg = f"Failed to write {filename}: {e}"
            logging.error(error_msg)
            self.validation_errors.append(error_msg)

    def _gather_automated_metadata(self, service_session: requests.Session):
        """
        Gather metadata that can be retrieved automatically or from a
        user defined directory
        """
        self._gather_subject_metadata(service_session)
        self._gather_procedures_metadata(service_session)
        self._gather_data_description_metadata(service_session)
        self._gather_processing_metadata()
        self._gather_instrument_metadata(service_session)

    def _gather_subject_metadata(self, service_session: requests.Session):
        """Gather subject metadata"""
        if self.settings.subject_settings is not None:
            contents = self.get_subject(service_session)
            if contents:
                self._write_json_file(
                    filename=Subject.default_filename(), contents=contents
                )

    def _gather_procedures_metadata(self, service_session: requests.Session):
        """Gather procedures metadata"""
        if self.settings.procedures_settings is not None:
            contents = self.get_procedures(service_session)
            if contents is not None:
                self._write_json_file(
                    filename=Procedures.default_filename(), contents=contents
                )

    def _gather_data_description_metadata(
        self, service_session: requests.Session
    ):
        """Gather data description metadata"""
        if self.settings.data_description_settings is not None:
            contents = self.get_data_description(service_session)
            if contents:
                self._write_json_file(
                    filename=DataDescription.default_filename(),
                    contents=contents
                )

    def _gather_processing_metadata(self):
        """Gather processing metadata"""
        if self.settings.processing_settings is not None:
            contents = self.get_processing_metadata()
            if contents is not None:
                self._write_json_file(
                    filename=Processing.default_filename(), contents=contents
                )

    def _gather_instrument_metadata(self, service_session: requests.Session):
        """Gather instrument metadata"""
        if self.settings.instrument_settings is not None:
            contents = self.get_instrument_metadata(service_session)
            if contents is not None:
                self._write_json_file(
                    filename=Instrument.default_filename(), contents=contents
                )

    def _setup_session_and_gather_metadata_from_service(self):
        """Create a session object and use it to get metadata from service"""
        retry_args = {
            "total": 3,
            "backoff_factor": 30,
            "status_forcelist": [500],
            "allowed_methods": ["GET"],
        }
        if "backoff_jitter" in signature(Retry.__init__).parameters:
            retry_args["backoff_jitter"] = 15

        retries = Retry(**retry_args)
        adapter = HTTPAdapter(max_retries=retries)
        service_session = requests.Session()
        service_session.mount("http://", adapter)
        
        try:
            self._gather_automated_metadata(service_session=service_session)
        finally:
            service_session.close()

    def _gather_file_based_metadata(self):
        """Gather metadata from user-provided files"""
        
        # Acquisition metadata (from file path)
        acq_contents = self.get_acquisition_metadata()
        if acq_contents:
            self._write_json_file(
                filename=Acquisition.default_filename(), contents=acq_contents
            )

        # Quality control metadata (optional, from user directory only)
        qc_contents = self.get_quality_control_metadata()
        if qc_contents:
            self._write_json_file(
                filename=QualityControl.default_filename(),
                contents=qc_contents
            )

    def run_job(self) -> None:
        """Run job and gather all metadata"""
        # Clear any previous validation errors
        self.validation_errors = []
        
        # Gather metadata from service and user directory
        self._setup_session_and_gather_metadata_from_service()
        
        # Gather metadata from user-provided file paths
        self._gather_file_based_metadata()
        
        # Create main metadata file if settings provided
        if self.settings.metadata_settings is not None:
            contents = self.get_main_metadata()
            output_path = (
                self.settings.directory_to_write_to /
                Metadata.default_filename()
            )
            try:
                with open(output_path, "w") as f:
                    json.dump(contents, f, indent=3)
            except Exception as e:
                error_msg = f"Failed to write metadata.nd.json: {e}"
                logging.error(error_msg)
                self.validation_errors.append(error_msg)
        
        # Report validation errors to user
        if self.validation_errors:
            logging.warning(
                "Validation errors encountered during metadata gathering:"
            )
            for error in self.validation_errors:
                logging.warning(f"  - {error}")
            logging.warning(
                "Metadata files have been created but may contain "
                "invalid data."
            )


if __name__ == "__main__":
    import argparse
    import sys
    
    sys_args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job-settings",
        required=True,
        type=str,
        help=(
            "Instead of init args the job settings can optionally be "
            "passed in as a json string in the command line."
        ),
    )
    cli_args = parser.parse_args(sys_args)
    main_job_settings = JobSettings.model_validate_json(cli_args.job_settings)
    job = GatherMetadataJob(settings=main_job_settings)
    job.run_job()
