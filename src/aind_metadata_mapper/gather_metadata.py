"""Module to gather metadata from different sources."""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List, Optional, Type

import requests
from aind_data_schema.base import AindCoreModel
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import (
    DataDescription,
    RawDataDescription,
)
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import PipelineProcess, Processing
from aind_data_schema.core.rig import Rig
from aind_data_schema.core.session import Session
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.pid_names import PIDName
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings


class SubjectSettings(BaseSettings):
    """Fields needed to retrieve subject metadata"""

    subject_id: str
    metadata_service_path: str = "subject"


class ProceduresSettings(BaseSettings):
    """Fields needed to retrieve procedures metadata"""

    subject_id: str
    metadata_service_path: str = "procedures"


class RawDataDescriptionSettings(BaseSettings):
    """Fields needed to retrieve data description metadata"""

    name: str
    project_name: str
    modality: List[Modality.ONE_OF]
    institution: Optional[Organization.ONE_OF] = Organization.AIND
    metadata_service_path: str = "funding"


class ProcessingSettings(BaseSettings):
    """Fields needed to retrieve processing metadata"""

    pipeline_process: PipelineProcess


class MetadataSettings(BaseSettings):
    """Fields needed to retrieve main Metadata"""

    name: str
    location: str
    subject_filepath: Optional[Path] = None
    data_description_filepath: Optional[Path] = None
    procedures_filepath: Optional[Path] = None
    session_filepath: Optional[Path] = None
    rig_filepath: Optional[Path] = None
    processing_filepath: Optional[Path] = None
    acquisition_filepath: Optional[Path] = None
    instrument_filepath: Optional[Path] = None


class JobSettings(BaseSettings):
    """Fields needed to gather all metadata"""

    metadata_service_domain: Optional[str] = None
    subject_settings: Optional[SubjectSettings] = None
    raw_data_description_settings: Optional[RawDataDescriptionSettings] = None
    procedures_settings: Optional[ProceduresSettings] = None
    processing_settings: Optional[ProcessingSettings] = None
    metadata_settings: Optional[MetadataSettings] = None
    directory_to_write_to: Path
    metadata_dir: Optional[Path] = Field(
        default=None,
        description="Optional path where user defined metadata files might be",
    )
    metadata_dir_force: bool = Field(
        default=False,
        description=(
            "Whether to override the user defined files in metadata_dir with "
            "those pulled from metadata service"
        ),
    )


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
        if self.settings.metadata_dir is not None:
            file_path_to_check = self.settings.metadata_dir / file_name
            if file_path_to_check.is_file():
                return True
            else:
                return False
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
        file_path = self.settings.metadata_dir / file_name
        # TODO: Add error handler in case json.load fails
        with open(file_path, "r") as f:
            contents = json.load(f)
        return contents

    def get_subject(self) -> dict:
        """Get subject metadata"""
        file_name = Subject.default_filename()
        should_use_service: bool = (
            not self.settings.metadata_dir_force
            or not self._does_file_exist_in_user_defined_dir(
                file_name=file_name
            )
        )
        if should_use_service:
            response = requests.get(
                self.settings.metadata_service_domain
                + f"/{self.settings.subject_settings.metadata_service_path}/"
                + f"{self.settings.subject_settings.subject_id}"
            )

            if response.status_code < 300 or response.status_code == 406:
                json_content = response.json()
                return json_content["data"]
            else:
                raise AssertionError(
                    f"Subject metadata is not valid! {response.json()}"
                )
        else:
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents

    def get_procedures(self) -> Optional[dict]:
        """Get procedures metadata"""
        file_name = Procedures.default_filename()
        should_use_service: bool = (
            not self.settings.metadata_dir_force
            or not self._does_file_exist_in_user_defined_dir(
                file_name=file_name
            )
        )
        if should_use_service:
            procedures_file_path = (
                self.settings.procedures_settings.metadata_service_path
            )
            response = requests.get(
                self.settings.metadata_service_domain
                + f"/{procedures_file_path}/"
                + f"{self.settings.procedures_settings.subject_id}"
            )

            if response.status_code < 300 or response.status_code == 406:
                json_content = response.json()
                return json_content["data"]
            else:
                logging.warning(
                    f"Procedures metadata is not valid! {response.json()}"
                )
                return None
        else:
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents

    def get_raw_data_description(self) -> dict:
        """Get raw data description metadata"""

        def get_funding_info(domain: str, url_path: str, project_name: str):
            """Utility method to retrieve funding info from metadata service"""
            response = requests.get("/".join([domain, url_path, project_name]))
            if response.status_code == 200:
                funding_info = [response.json().get("data")]
            elif response.status_code == 300:
                funding_info = response.json().get("data")
            else:
                funding_info = []
            investigators = set()
            for f in funding_info:
                project_fundees = f.get("fundee", "").split(",")
                pid_names = [
                    PIDName(name=p.strip()).model_dump_json()
                    for p in project_fundees
                ]
                if project_fundees is not [""]:
                    investigators.update(pid_names)
            investigators = [
                PIDName.model_validate_json(i) for i in investigators
            ]
            investigators.sort(key=lambda x: x.name)
            return funding_info, investigators

        # Returns a dict with platform, subject_id, and acq_datetime
        file_name = RawDataDescription.default_filename()
        should_use_service: bool = (
            not self.settings.metadata_dir_force
            or not self._does_file_exist_in_user_defined_dir(
                file_name=file_name
            )
        )
        if should_use_service:
            basic_settings = RawDataDescription.parse_name(
                name=self.settings.raw_data_description_settings.name
            )
            ds_settings = self.settings.raw_data_description_settings
            funding_source, investigator_list = get_funding_info(
                self.settings.metadata_service_domain,
                ds_settings.metadata_service_path,
                self.settings.raw_data_description_settings.project_name,
            )

            try:
                institution = (
                    self.settings.raw_data_description_settings.institution
                )
                modality = self.settings.raw_data_description_settings.modality
                return json.loads(
                    RawDataDescription(
                        name=self.settings.raw_data_description_settings.name,
                        institution=institution,
                        modality=modality,
                        funding_source=funding_source,
                        investigators=investigator_list,
                        **basic_settings,
                    ).model_dump_json()
                )
            except ValidationError:
                institution = (
                    self.settings.raw_data_description_settings.institution
                )
                modality = self.settings.raw_data_description_settings.modality
                return json.loads(
                    RawDataDescription.model_construct(
                        name=self.settings.raw_data_description_settings.name,
                        institution=institution,
                        modality=modality,
                        funding_source=funding_source,
                        investigators=investigator_list,
                        **basic_settings,
                    ).model_dump_json()
                )
        else:
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents

    def get_processing_metadata(self):
        """Get processing metadata"""

        file_name = Processing.default_filename()
        should_use_service: bool = (
            not self.settings.metadata_dir_force
            or not self._does_file_exist_in_user_defined_dir(
                file_name=file_name
            )
        )
        if should_use_service:
            processing_instance = Processing(
                processing_pipeline=(
                    self.settings.processing_settings.pipeline_process
                )
            )
            return json.loads(processing_instance.model_dump_json())
        else:
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents

    def get_session_metadata(self) -> Optional[dict]:
        """Get session metadata"""
        file_name = Session.default_filename()
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents
        else:
            return None

    def get_rig_metadata(self) -> Optional[dict]:
        """Get rig metadata"""
        file_name = Rig.default_filename()
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents
        else:
            return None

    def get_acquisition_metadata(self) -> Optional[dict]:
        """Get acquisition metadata"""
        file_name = Acquisition.default_filename()
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents
        else:
            return None

    def get_instrument_metadata(self) -> Optional[dict]:
        """Get instrument metadata"""
        file_name = Instrument.default_filename()
        if self._does_file_exist_in_user_defined_dir(file_name=file_name):
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
            return contents
        else:
            return None

    def get_main_metadata(self) -> Metadata:
        """Get main Metadata model"""

        def load_model(
            filepath: Optional[Path], model: Type[AindCoreModel]
        ) -> Optional[AindCoreModel]:
            """
            Validates contents of file with an AindCoreModel
            Parameters
            ----------
            filepath : Optional[Path]
            model : Type[AindCoreModel]

            Returns
            -------
            Optional[AindCodeModel]

            """
            if filepath is not None and filepath.is_file():
                with open(filepath, "r") as f:
                    contents = json.load(f)
                try:
                    output = model.model_validate_json(json.dumps(contents))
                except (ValidationError, AttributeError, ValueError, KeyError):
                    output = model.model_construct(**contents)

                return output
            else:
                return None

        subject = load_model(
            self.settings.metadata_settings.subject_filepath, Subject
        )
        data_description = load_model(
            self.settings.metadata_settings.data_description_filepath,
            DataDescription,
        )
        procedures = load_model(
            self.settings.metadata_settings.procedures_filepath, Procedures
        )
        session = load_model(
            self.settings.metadata_settings.session_filepath, Session
        )
        rig = load_model(self.settings.metadata_settings.rig_filepath, Rig)
        acquisition = load_model(
            self.settings.metadata_settings.acquisition_filepath, Acquisition
        )
        instrument = load_model(
            self.settings.metadata_settings.instrument_filepath, Instrument
        )
        processing = load_model(
            self.settings.metadata_settings.processing_filepath, Processing
        )

        try:
            metadata = Metadata(
                name=self.settings.metadata_settings.name,
                location=self.settings.metadata_settings.location,
                subject=subject,
                data_description=data_description,
                procedures=procedures,
                session=session,
                rig=rig,
                processing=processing,
                acquisition=acquisition,
                instrument=instrument,
            )
            return metadata
        except (ValidationError, ValueError, AttributeError, KeyError):
            metadata = Metadata.model_construct(
                name=self.settings.metadata_settings.name,
                location=self.settings.metadata_settings.location,
                subject=subject,
                data_description=data_description,
                procedures=procedures,
                session=session,
                rig=rig,
                processing=processing,
                acquisition=acquisition,
                instrument=instrument,
            )
            return metadata

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
        with open(output_path, "w") as f:
            json.dump(contents, f, indent=3)

    def _gather_automated_metadata(self):
        """Gather metadata that can be retrieved automatically or from a
        user defined directory"""
        if self.settings.subject_settings is not None:
            contents = self.get_subject()
            self._write_json_file(
                filename=Subject.default_filename(), contents=contents
            )
        if self.settings.procedures_settings is not None:
            contents = self.get_procedures()
            if contents is not None:
                self._write_json_file(
                    filename=Procedures.default_filename(), contents=contents
                )
        if self.settings.raw_data_description_settings is not None:
            contents = self.get_raw_data_description()
            self._write_json_file(
                filename=DataDescription.default_filename(), contents=contents
            )
        if self.settings.processing_settings is not None:
            contents = self.get_processing_metadata()
            self._write_json_file(
                filename=Processing.default_filename(), contents=contents
            )

    def _gather_non_automated_metadata(self):
        """Gather metadata that cannot yet be retrieved automatically but
        may be in a user defined directory."""
        if self.settings.metadata_settings is None:
            rig_contents = self.get_rig_metadata()
            if rig_contents:
                self._write_json_file(
                    filename=Rig.default_filename(), contents=rig_contents
                )
            session_contents = self.get_session_metadata()
            if session_contents:
                self._write_json_file(
                    filename=Session.default_filename(),
                    contents=session_contents,
                )
            acq_contents = self.get_acquisition_metadata()
            if acq_contents:
                self._write_json_file(
                    filename=Acquisition.default_filename(),
                    contents=acq_contents,
                )
            instrument_contents = self.get_instrument_metadata()
            if instrument_contents:
                self._write_json_file(
                    filename=Instrument.default_filename(),
                    contents=instrument_contents,
                )

    def run_job(self) -> None:
        """Run job"""
        self._gather_automated_metadata()
        self._gather_non_automated_metadata()
        if self.settings.metadata_settings is not None:
            metadata = self.get_main_metadata()
            # TODO: may need to update aind-data-schema write standard file
            #  class
            output_path = (
                self.settings.directory_to_write_to
                / Metadata.default_filename()
            )
            contents = json.loads(metadata.model_dump_json(by_alias=True))
            with open(output_path, "w") as f:
                json.dump(
                    contents,
                    f,
                    indent=3,
                    ensure_ascii=False,
                    sort_keys=True,
                )


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job-settings",
        required=True,
        type=str,
        help=(
            r"""
            Instead of init args the job settings can optionally be passed in
            as a json string in the command line.
            """
        ),
    )
    cli_args = parser.parse_args(sys_args)
    main_job_settings = JobSettings.model_validate_json(cli_args.job_settings)
    job = GatherMetadataJob(settings=main_job_settings)
    job.run_job()
