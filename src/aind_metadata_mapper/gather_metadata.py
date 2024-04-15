"""Module to gather metadata from different sources."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Type

import requests
from aind_data_schema.base import AindCoreModel
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import (
    DataDescription,
    Funding,
    RawDataDescription,
)
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import (
    DataProcess,
    PipelineProcess,
    Processing,
)
from aind_data_schema.core.rig import Rig
from aind_data_schema.core.session import Session
from aind_data_schema.core.subject import Subject
from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.organizations import Organization
from aind_data_schema.models.pid_names import PIDName
from aind_data_schema.models.process_names import ProcessName
from pydantic import ValidationError
from pydantic_settings import BaseSettings


class SubjectSettings(BaseSettings):
    """Fields needed to retrieve subject metadata"""

    subject_id: str


class ProceduresSettings(BaseSettings):
    """Fields needed to retrieve procedures metadata"""

    subject_id: str


class DataDescriptionSettings(BaseSettings):
    """Fields needed to retrieve data description metadata"""

    investigators: List[PIDName]
    data_description_name: str
    modality: List[Modality]
    funding_source: Optional[Tuple] = (Funding(funder=Organization.AI),)
    institution: Optional[Organization] = Organization.AIND


class ProcessingSettings(BaseSettings):
    """Fields needed to retrieve processing metadata"""

    process_name: ProcessName
    process_start_date_time: datetime
    process_end_date_time: datetime
    process_input_location: str
    process_output_location: str
    process_code_url: str
    process_parameters: dict = {}
    process_processor_full_name: str
    process_software_version: str
    process_notes: Optional[str] = None


class MetadataSettings(BaseSettings):
    """Fields needed to retrieve main Metadata"""

    name: str
    location: str
    subject_filepath: Optional[Path]
    data_description_filepath: Optional[Path]
    procedures_filepath: Optional[Path]
    session_filepath: Optional[Path]
    rig_filepath: Optional[Path]
    processing_filepath: Optional[Path]
    acquisition_filepath: Optional[Path]
    instrument_filepath: Optional[Path]


class JobSettings(BaseSettings):
    """Fields needed to gather all metadata"""

    subject_settings: Optional[SubjectSettings] = None
    data_description_settings: Optional[DataDescriptionSettings] = None
    procedures_settings: Optional[ProceduresSettings] = None
    processing_settings: Optional[ProcessingSettings] = None
    metadata_settings: Optional[MetadataSettings] = None
    metadata_service_url: Optional[str] = None
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
            self.settings.metadata_service_url
            + f"/subject/{self.settings.subject_settings.subject_id}"
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
            self.settings.metadata_service_url
            + f"/procedures/{self.settings.procedures_settings.subject_id}"
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
        basic_settings = RawDataDescription.parse_name(name=self.settings.name)

        try:
            return json.loads(
                RawDataDescription(
                    name=self.settings.name,
                    institution=self.settings.institution,
                    modality=self.settings.modality,
                    funding_source=self.settings.funding_source_list,
                    investigators=self.settings.investigators,
                    **basic_settings,
                ).model_dump_json()
            )
        except ValidationError:
            return json.loads(
                RawDataDescription.model_construct(
                    name=self.settings.name,
                    institution=self.settings.institution,
                    modality=self.settings.modality,
                    funding_source=self.settings.funding_source_list,
                    investigators=self.settings.investigators,
                    **basic_settings,
                ).model_dump_json()
            )

    def get_processing_metadata(self):
        """Get processing metadata"""
        data_processing_instance = DataProcess(
            name=self.settings.process_name,
            software_version=self.settings.process_software_version,
            start_date_time=self.settings.process_start_date_time,
            end_date_time=self.settings.process_end_date_time,
            input_location=self.settings.process_input_location,
            output_location=self.settings.process_output_location,
            code_url=self.settings.process_code_url,
            parameters=self.settings.process_parameters,
            notes=self.settings.process_notes,
        )

        pipeline_process_instance = PipelineProcess(
            data_processes=[data_processing_instance],
            processor_full_name=self.settings.process_processor_full_name,
        )

        try:
            processing_instance = Processing(
                processing_pipeline=pipeline_process_instance
            )
            return json.loads(processing_instance.model_dump_json())
        except ValidationError:
            processing_instance = Processing.model_construct(
                processing_pipeline=pipeline_process_instance
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

        processing_contents = self.get_processing_metadata()
        try:
            processing = Processing.model_validate(processing_contents)
        except ValidationError:
            processing = Processing.model_construct(processing_contents)
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
            self._write_json_file(filename="subject.json", contents=contents)
        if self.settings.procedures_settings is not None:
            contents = self.get_procedures()
            self._write_json_file(
                filename="procedures.json", contents=contents
            )
        if self.settings.data_description_settings is not None:
            contents = self.get_raw_data_description()
            self._write_json_file(
                filename="data_description.json", contents=contents
            )
        if self.settings.processing_settings is not None:
            contents = self.get_processing_metadata()
            self._write_json_file(
                filename="processing.json", contents=contents
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
