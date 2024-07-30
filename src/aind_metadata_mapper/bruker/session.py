"""Sets up the MRI ingest ETL"""

import argparse
import json
import logging
import sys
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Union
from zoneinfo import ZoneInfo

from aind_data_schema.components.coordinates import (
    Rotation3dTransform,
    Scale3dTransform,
    Translation3dTransform,
)
from aind_data_schema.components.devices import (
    MagneticStrength,
    Scanner,
    ScannerLocation,
)
from aind_data_schema.core.session import (
    MRIScan,
    MriScanSequence,
    ScanType,
    Session,
    Stream,
    SubjectPosition,
)
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import TimeUnit
from bruker2nifti._metadata import BrukerMetadata
from pydantic import Field
from pydantic_settings import BaseSettings

from aind_metadata_mapper.core import GenericEtl, JobResponse


class JobSettings(BaseSettings):
    """Data that needs to be input by user."""

    data_path: Path
    output_directory: Optional[Path] = Field(
        default=None,
        description=(
            "Directory where to save the json file to. If None, then json"
            " contents will be returned in the Response message."
        ),
    )
    experimenter_full_name: List[str]
    protocol_id: str = Field(default="", description="Protocol ID")
    collection_tz: str = Field(
        default="America/Los_Angeles",
        description="Timezone string of the collection site",
    )
    session_type: str
    primary_scan_number: int
    setup_scan_number: int
    scanner_name: str
    scan_location: ScannerLocation
    magnetic_strength: MagneticStrength
    subject_id: str
    iacuc_protocol: str
    session_notes: str


DATETIME_FORMAT = "%H:%M:%S %d %b %Y"
LENGTH_FORMAT = "%Hh%Mm%Ss%fms"


class MRIEtl(GenericEtl[JobSettings]):
    """Class for MRI ETL process."""

    def __init__(self, job_settings: Union[JobSettings, str]):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        job_settings: Union[JobSettings, str]
          Variables for a particular session
        """

        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        else:
            job_settings_model = job_settings
        super().__init__(job_settings=job_settings_model)

    @classmethod
    def from_args(cls, args: list):
        """
        Adds ability to construct settings from a list of arguments.
        Parameters
        ----------
        args : list
        A list of command line arguments to parse.
        """

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-u",
            "--job-settings",
            required=True,
            type=json.loads,
            help=(
                r"""
                Custom settings defined by the user defined as a json
                 string. For example: -u
                 '{"experimenter_full_name":["John Smith","Jane Smith"],
                 "subject_id":"12345",
                 "session_start_time":"2023-10-10T10:10:10",
                 "session_end_time":"2023-10-10T18:10:10",
                 "project":"my_project"}
                """
            ),
        )
        job_args = parser.parse_args(args)
        job_settings_from_args = JobSettings(**job_args.job_settings)
        return cls(
            job_settings=job_settings_from_args,
        )

    def _extract(self) -> BrukerMetadata:
        """Extract the data from the bruker files."""

        metadata = BrukerMetadata(self.job_settings.data_path)
        metadata.parse_scans()
        metadata.parse_subject()

        # self.n_scans = self.metadata.list_scans()
        return metadata

    def _transform(self, input_metadata: BrukerMetadata) -> Session:
        """Transform the data into the AIND data schema."""

        return self.load_mri_session(
            experimenter=self.job_settings.experimenter_full_name,
            primary_scan_number=self.job_settings.primary_scan_number,
            setup_scan_number=self.job_settings.setup_scan_number,
            scan_data=input_metadata.scan_data,
            subject_data=input_metadata.subject_data,
        )

    def run_job(self) -> JobResponse:
        """Run the job and return the response."""

        extracted = self._extract()
        transformed = self._transform(extracted)

        job_response = self._load(transformed, self.job_settings.output_directory)

        return job_response

    def load_mri_session(
        self,
        scan_data,
        subject_data,
        experimenter: List[str],
        primary_scan_number: int,
        setup_scan_number: int,
    ) -> Session:
        """Load the MRI session data into the AIND data schema."""

        scans = []
        for scan in scan_data.keys():
            scan_type = "3D Scan"
            if scan == setup_scan_number:
                scan_type = "Set Up"
            primary_scan = False
            if scan == primary_scan_number:
                primary_scan = True
            new_scan = self.make_model_from_scan(
                scan_index=scan,
                scan_type=scan_type,
                primary_scan=primary_scan,
                scan_data=scan_data,
                subject_data=subject_data,
            )
            logging.info(f"loaded scan {new_scan}")

            scans.append(new_scan)

        logging.info(f"loaded scans: {scans}")

        start_time = datetime.strptime(
            scan_data[list(scan_data.keys())[0]]["acqp"]["ACQ_time"],
            DATETIME_FORMAT,
        ).replace(tzinfo=ZoneInfo(self.job_settings.collection_tz))
        start_time = start_time.astimezone(ZoneInfo("UTC"))
        final_scan_start = datetime.strptime(
            scan_data[list(scan_data.keys())[-1]]["acqp"]["ACQ_time"],
            DATETIME_FORMAT,
        ).replace(tzinfo=ZoneInfo(self.job_settings.collection_tz))
        final_scan_duration = datetime.strptime(
            scan_data[list(scan_data.keys())[-1]]["method"]["ScanTimeStr"],
            LENGTH_FORMAT,
        ).replace(tzinfo=ZoneInfo(self.job_settings.collection_tz))
        end_time = final_scan_start + timedelta(
            hours=final_scan_duration.hour,
            minutes=final_scan_duration.minute,
            seconds=final_scan_duration.second,
            microseconds=final_scan_duration.microsecond,
        )
        end_time = end_time.astimezone(ZoneInfo("UTC"))

        stream = Stream(
            stream_start_time=start_time,
            stream_end_time=end_time,
            mri_scans=scans,
            stream_modalities=[Modality.MRI],
        )

        return Session(
            subject_id=self.job_settings.subject_id,
            session_start_time=start_time,
            session_end_time=end_time,
            session_type=self.job_settings.session_type,
            experimenter_full_name=experimenter,
            protocol_id=[self.job_settings.protocol_id],
            iacuc_protocol=self.job_settings.iacuc_protocol,
            data_streams=[stream],
            rig_id=self.job_settings.scanner_name,
            mouse_platform_name="NA",
            active_mouse_platform=False,
            notes=self.job_settings.session_notes,
        )

    @staticmethod
    def get_position(subject_data):
        """Get the position of the subject."""
        subj_pos = subject_data["SUBJECT_position"]
        if "supine" in subj_pos.lower():
            return "Supine"
        elif "prone" in subj_pos.lower():
            return "Prone"
        return subj_pos

    @staticmethod
    def get_scan_sequence_type(method):
        """Get the scan sequence type."""
        if "RARE" in method["Method"]:
            return MriScanSequence(method["Method"])

        return MriScanSequence.OTHER

    @staticmethod
    def get_rotation(visu_pars):
        """Get the rotation."""
        rotation = visu_pars.get("VisuCoreOrientation")
        if rotation.shape == (1, 9):
            return Rotation3dTransform(rotation=rotation.tolist()[0])
        return None

    @staticmethod
    def get_translation(visu_pars):
        """Get the translation."""
        translation = visu_pars.get("VisuCorePosition")
        if translation.shape == (1, 3):
            return Translation3dTransform(translation=translation.tolist()[0])
        return None

    @staticmethod
    def get_scale(method):
        """Get the scale."""
        scale = method.get("SpatResol")
        if not isinstance(scale, list):
            scale = scale.tolist()
        if len(scale) == 3:
            return Scale3dTransform(scale=scale)
        return None

    def make_model_from_scan(
        self,
        scan_index: str,
        scan_type,
        primary_scan: bool,
        scan_data,
        subject_data,
    ) -> MRIScan:
        """load scan data into the AIND data schema."""

        logging.info(f"loading scan {scan_index}")

        cur_visu_pars = scan_data[scan_index]["recons"]["1"]["visu_pars"]
        cur_method = scan_data[scan_index]["method"]

        subj_pos = self.get_position(subject_data)

        scan_sequence = self.get_scan_sequence_type(cur_method)

        notes = None
        if scan_sequence == MriScanSequence.OTHER:
            notes = f"Scan sequence {cur_method['Method']} not recognized"

        rare_factor = cur_method.get("RareFactor", None)

        eff_echo_time = cur_method.get("EffectiveTE", None)
        if eff_echo_time is not None:
            eff_echo_time = Decimal(eff_echo_time)

        rotation = self.get_rotation(cur_visu_pars)

        translation = self.get_translation(cur_visu_pars)

        scale = self.get_scale(cur_method)

        try:
            return MRIScan(
                scan_index=scan_index,
                scan_type=ScanType(scan_type),
                primary_scan=primary_scan,
                mri_scanner=Scanner(
                    name=self.job_settings.scanner_name,
                    scanner_location=self.job_settings.scan_location,
                    magnetic_strength=self.job_settings.magnetic_strength,
                    magnetic_strength_unit="T",
                ),
                scan_sequence_type=scan_sequence,
                rare_factor=rare_factor,
                echo_time=cur_method["EchoTime"],
                effective_echo_time=eff_echo_time,
                echo_time_unit=TimeUnit.MS,  # what do we want here?
                repetition_time=cur_method["RepetitionTime"],
                repetition_time_unit=TimeUnit.MS,  # ditto
                vc_orientation=rotation,
                vc_position=translation,
                subject_position=SubjectPosition(subj_pos),
                voxel_sizes=scale,
                processing_steps=[],
                additional_scan_parameters={},
                notes=notes,
            )
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error(f"Error loading scan {scan_index}: {e}")


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    etl = MRIEtl.from_args(sys_args)
    etl.run_job()
