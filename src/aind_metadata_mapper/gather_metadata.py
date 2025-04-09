"""Module to gather metadata from different sources."""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional, Type

import requests
from aind_data_schema.base import AindCoreModel
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import (
    DataDescription,
    RawDataDescription,
)
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.metadata import Metadata, MetadataStatus
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import PipelineProcess, Processing
from aind_data_schema.core.quality_control import QualityControl
from aind_data_schema.core.rig import Rig
from aind_data_schema.core.session import Session
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.pid_names import PIDName
from pydantic import ValidationError
from pydantic_core import PydanticSerializationError

from aind_metadata_mapper.bergamo.models import (
    JobSettings as BergamoSessionJobSettings,
)
from aind_metadata_mapper.bergamo.session import BergamoEtl
from aind_metadata_mapper.bruker.models import (
    JobSettings as BrukerSessionJobSettings,
)
from aind_metadata_mapper.bruker.session import MRIEtl
from aind_metadata_mapper.fip.models import (
    JobSettings as FipSessionJobSettings,
)
from aind_metadata_mapper.fip.session import FIBEtl
from aind_metadata_mapper.mesoscope.models import (
    JobSettings as MesoscopeSessionJobSettings,
)
from aind_metadata_mapper.mesoscope.session import MesoscopeEtl
from aind_metadata_mapper.models import JobSettings
from aind_metadata_mapper.open_ephys.camstim_ephys_session import (
    CamstimEphysSessionEtl,
)
from aind_metadata_mapper.open_ephys.models import (
    JobSettings as OpenEphysJobSettings,
)
from aind_metadata_mapper.smartspim.acquisition import SmartspimETL


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
        # convert metadata_str to Path object
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
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
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
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
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
                    f"Procedures metadata is not valid! {response.status_code}"
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
            parsed_funding_info = []
            for f in funding_info:
                project_investigators = (
                    ""
                    if f.get("investigators", None) is None
                    else f.get("investigators", "").split(",")
                )
                investigators_pid_names = [
                    PIDName(name=p.strip()).model_dump_json()
                    for p in project_investigators
                ]
                if project_investigators is not [""]:
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

        # Returns a dict with platform, subject_id, and acq_datetime
        file_name = RawDataDescription.default_filename()
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            # Returns a dictionary with name, subject_id, and creation_time
            basic_settings = RawDataDescription.parse_name(
                name=self.settings.raw_data_description_settings.name
            )
            ds_settings = self.settings.raw_data_description_settings
            project_name = (
                self.settings.raw_data_description_settings.project_name
            )
            funding_source, investigator_list = get_funding_info(
                self.settings.metadata_service_domain,
                ds_settings.metadata_service_path,
                project_name,
            )

            try:
                institution = (
                    self.settings.raw_data_description_settings.institution
                )
                modality = self.settings.raw_data_description_settings.modality
                return json.loads(
                    RawDataDescription(
                        project_name=project_name,
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
                        project_name=project_name,
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
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            try:
                processing_pipeline = PipelineProcess.model_validate_json(
                    json.dumps(
                        self.settings.processing_settings.pipeline_process
                    )
                )
                processing_instance = Processing(
                    processing_pipeline=processing_pipeline
                )
            except ValidationError:
                processing_pipeline = PipelineProcess.model_construct(
                    **self.settings.processing_settings.pipeline_process
                )
                processing_instance = Processing.model_construct(
                    processing_pipeline=processing_pipeline
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
        elif self.settings.session_settings is not None:
            session_settings = self.settings.session_settings.job_settings
            if isinstance(session_settings, BergamoSessionJobSettings):
                session_job = BergamoEtl(job_settings=session_settings)
            elif isinstance(session_settings, BrukerSessionJobSettings):
                session_job = MRIEtl(job_settings=session_settings)
            elif isinstance(session_settings, FipSessionJobSettings):
                session_job = FIBEtl(job_settings=session_settings)
            elif isinstance(session_settings, MesoscopeSessionJobSettings):
                session_job = MesoscopeEtl(job_settings=session_settings)
            elif isinstance(session_settings, OpenEphysJobSettings):
                session_job = CamstimEphysSessionEtl(
                    job_settings=session_settings
                )
            else:
                raise ValueError("Unknown session job settings class!")
            job_response = session_job.run_job()
            if job_response.status_code != 500:
                return json.loads(job_response.data)
            else:
                return None
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

    def get_quality_control_metadata(self) -> Optional[dict]:
        """Get quality_control metadata"""
        file_name = QualityControl.default_filename()
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
        elif self.settings.acquisition_settings is not None:
            acquisition_job = SmartspimETL(
                job_settings=self.settings.acquisition_settings.job_settings
            )
            job_response = acquisition_job.run_job()
            if job_response.status_code != 500:
                return json.loads(job_response.data)
            else:
                return None
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

    def _get_location(self, metadata_status: MetadataStatus) -> str:
        """
        Get location where to upload the data to.
        Parameters
        ----------
        metadata_status : Metadata

        Returns
        -------
        str

        """
        location_map = self.settings.metadata_settings.location_map
        if self.settings.metadata_settings.location is not None:
            return self.settings.metadata_settings.location
        elif (
            location_map is not None
            and metadata_status is MetadataStatus.VALID
        ):
            return location_map[MetadataStatus.VALID]
        elif (
            location_map is not None
            and metadata_status != MetadataStatus.VALID
            and MetadataStatus.INVALID in location_map.keys()
        ):
            invalid_value = location_map[MetadataStatus.INVALID]
            return location_map.get(metadata_status, invalid_value)
        else:
            raise ValueError(
                f"Unable to set location from "
                f"{self.settings.metadata_settings}!"
            )

    def get_main_metadata(self) -> dict:
        """Get serialized main Metadata model"""

        def load_model(
            filepath: Optional[Path], model: Type[AindCoreModel]
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
                with open(filepath, "r") as f:
                    contents = json.load(f)
                try:
                    valid_model = model.model_validate_json(
                        json.dumps(contents)
                    )
                    output = json.loads(valid_model.model_dump_json())
                except (
                    ValidationError,
                    AttributeError,
                    ValueError,
                    KeyError,
                    PydanticSerializationError,
                ):
                    output = contents

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
        quality_control = load_model(
            self.settings.metadata_settings.quality_control_filepath,
            QualityControl,
        )
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
                location="placeholder",
                subject=subject,
                data_description=data_description,
                procedures=procedures,
                session=session,
                rig=rig,
                processing=processing,
                acquisition=acquisition,
                instrument=instrument,
                quality_control=quality_control,
            )
            location = self._get_location(
                metadata_status=metadata.metadata_status
            )
            metadata.location = location
            metadata_json = json.loads(metadata.model_dump_json(by_alias=True))
            return metadata_json
        except Exception as e:
            logging.warning(f"Issue with metadata construction! {e.args}")
            # Set basic parameters
            location = self._get_location(
                metadata_status=MetadataStatus.INVALID
            )
            metadata = Metadata(
                name=self.settings.metadata_settings.name,
                location=location,
            )
            metadata_json = json.loads(metadata.model_dump_json(by_alias=True))
            # Attach dict objects
            metadata_json["subject"] = subject
            metadata_json["data_description"] = data_description
            metadata_json["procedures"] = procedures
            metadata_json["session"] = session
            metadata_json["rig"] = rig
            metadata_json["processing"] = processing
            metadata_json["acquisition"] = acquisition
            metadata_json["instrument"] = instrument
            metadata_json["quality_control"] = quality_control
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
            contents = self.get_main_metadata()
            # TODO: may need to update aind-data-schema write standard file
            #  class
            output_path = (
                self.settings.directory_to_write_to
                / Metadata.default_filename()
            )
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
