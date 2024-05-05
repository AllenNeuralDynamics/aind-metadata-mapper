"""Module to gather metadata from different sources."""

import argparse
import json
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
from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.organizations import Organization
from aind_data_schema.models.pid_names import PIDName
from pydantic import ValidationError
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

    def get_subject(self) -> dict:
        """Get subject metadata"""
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

    def get_procedures(self) -> dict:
        """Get procedures metadata"""
        response = requests.get(
            self.settings.metadata_service_domain
            + f"/{self.settings.procedures_settings.metadata_service_path}/"
            + f"{self.settings.procedures_settings.subject_id}"
        )

        if response.status_code < 300 or response.status_code == 406:
            json_content = response.json()
            return json_content["data"]
        else:
            raise AssertionError(
                f"Procedures metadata is not valid! {response.json()}"
            )

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
        basic_settings = RawDataDescription.parse_name(
            name=self.settings.raw_data_description_settings.name
        )
        funding_source, investigator_list = get_funding_info(
            self.settings.metadata_service_domain,
            self.settings.raw_data_description_settings.metadata_service_path,
            self.settings.raw_data_description_settings.project_name,
        )

        try:
            return json.loads(
                RawDataDescription(
                    name=self.settings.raw_data_description_settings.name,
                    institution=(
                        self.settings.raw_data_description_settings.institution
                    ),
                    modality=(
                        self.settings.raw_data_description_settings.modality
                    ),
                    funding_source=funding_source,
                    investigators=investigator_list,
                    **basic_settings,
                ).model_dump_json()
            )
        except ValidationError:
            return json.loads(
                RawDataDescription.model_construct(
                    name=self.settings.raw_data_description_settings.name,
                    institution=(
                        self.settings.raw_data_description_settings.institution
                    ),
                    modality=(
                        self.settings.raw_data_description_settings.modality
                    ),
                    funding_source=funding_source,
                    investigators=investigator_list,
                    **basic_settings,
                ).model_dump_json()
            )

    def get_processing_metadata(self):
        """Get processing metadata"""

        processing_instance = Processing(
            processing_pipeline=(
                self.settings.processing_settings.pipeline_process
            )
        )
        return json.loads(processing_instance.model_dump_json())

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
            if filepath is not None:
                with open(filepath, "r") as f:
                    contents = json.load(f)
                try:
                    output = model.model_validate_json(json.dumps(contents))
                except ValidationError:
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

    def run_job(self) -> None:
        """Run job"""
        if self.settings.subject_settings is not None:
            contents = self.get_subject()
            self._write_json_file(
                filename=Subject.default_filename(), contents=contents
            )
        if self.settings.procedures_settings is not None:
            contents = self.get_procedures()
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
        if self.settings.metadata_settings is not None:
            metadata = self.get_main_metadata()
            metadata.write_standard_file(
                output_directory=self.settings.directory_to_write_to
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
