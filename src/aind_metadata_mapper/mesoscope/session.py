"""Mesoscope ETL"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Union

import tifffile
from aind_data_schema.core.session import FieldOfView, Session, Stream
from aind_data_schema.models.modalities import Modality
from PIL import Image
from PIL.TiffTags import TAGS
from pydantic import Field
from pydantic_settings import BaseSettings

from aind_metadata_mapper.core import GenericEtl


class JobSettings(BaseSettings):
    """Data to be entered by the user."""

    # TODO: for now this will need to be directly input by the user.
    #  In the future, the workflow sequencing engine should be able to put
    #  this in a json or we can extract it from SLIMS
    input_source: Path
    behavior_source: Path
    output_directory: Path
    session_start_time: datetime
    session_end_time: datetime
    subject_id: str
    project: str
    iacuc_protocol: str = "2115"
    magnification: str = "16x"
    fov_coordinate_ml: float = 1.5
    fov_coordinate_ap: float = 1.5
    fov_reference: str = "Bregma"
    experimenter_full_name: List[str] = Field(
        ..., title="Full name of the experimenter"
    )
    mouse_platform_name: str = "disc"


class MesoscopeEtl(GenericEtl[JobSettings]):
    """Class to manage transforming mesoscope platform json and metadata into
    a Session model."""

    _STRUCTURE_LOOKUP_DICT = {
        385: "VISp",
        394: "VISam",
        402: "VISal",
        409: "VISl",
        417: "VISrl",
        533: "VISpm",
        312782574: "VISli",
    }

    def __init__(
        self,
        job_settings: Union[JobSettings, str],
    ):
        """Class constructor for Mesoscope etl job"""
        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        else:
            job_settings_model = job_settings
        super().__init__(job_settings=job_settings_model)

    def _read_metadata(self, tiff_path: Path):
        """
        Calls tifffile.read_scanimage_metadata on the specified
        path and returns teh result. This method was factored
        out so that it could be easily mocked in unit tests.
        """
        if not tiff_path.is_file():
            raise ValueError(
                f"{tiff_path.resolve().absolute()} " "is not a file"
            )
        with open(tiff_path, "rb") as tiff:
            file_handle = tifffile.FileHandle(tiff)
            file_contents = tifffile.read_scanimage_metadata(file_handle)
        return file_contents

    def _extract(self) -> dict:
        """extract data from the platform json file and tiff file (in the
        future).
        If input source is a file, will extract the data from the file.
        The input source is a directory, will extract the data from the
        directory.

        Returns
        -------
        dict
            The extracted data from the platform json file.
        """
        # The pydantic models will validate that the user inputs a Path.
        # We can add validators there if we want to coerce strings to Paths.
        input_source = self.job_settings.input_source
        behavior_source = self.job_settings.behavior_source
        session_metadata = {}
        if behavior_source.is_dir():
            # deterministic order
            for ftype in sorted(list(behavior_source.glob("*json"))):
                if (
                    "Behavior" in ftype.stem
                    or "Eye" in ftype.stem
                    or "Face" in ftype.stem
                ):
                    with open(ftype, "r") as f:
                        session_metadata[ftype.stem] = json.load(f)
        else:
            raise ValueError("Behavior source must be a directory")
        if input_source.is_dir():
            input_source = next(input_source.glob("*platform.json"), "")
            if (
                isinstance(input_source, str) and input_source == ""
            ) or not input_source.exists():
                raise ValueError("No platform json file found in directory")
        with open(input_source, "r") as f:
            session_metadata["platform"] = json.load(f)
        return session_metadata

    def _transform(self, extracted_source: dict) -> Session:
        """Transform the platform data into a session object

        Parameters
        ----------
        extracted_source : dict
            Extracted data from the camera jsons and platform json.
        Returns
        -------
        Session
            The session object
        """
        imaging_plane_groups = extracted_source["platform"][
            "imaging_plane_groups"
        ]
        timeseries = next(
            self.job_settings.input_source.glob("*timeseries*.tiff"), ""
        )
        meta = self._read_metadata(timeseries)
        fovs = []
        data_streams = []
        for group in imaging_plane_groups:
            for plane in group["imaging_planes"]:
                fov = FieldOfView(
                    index=int(group["local_z_stack_tif"].split(".")[0][-1]),
                    fov_coordinate_ml=self.job_settings.fov_coordinate_ml,
                    fov_coordinate_ap=self.job_settings.fov_coordinate_ap,
                    fov_reference=self.job_settings.fov_reference,
                    magnification=self.job_settings.magnification,
                    fov_scale_factor=meta[0]["SI.hRoiManager.scanZoomFactor"],
                    imaging_depth=plane["targeted_depth"],
                    targeted_structure=self._STRUCTURE_LOOKUP_DICT[
                        plane["targeted_structure_id"]
                    ],
                    fov_width=meta[0]["SI.hRoiManager.pixelsPerLine"],
                    fov_height=meta[0]["SI.hRoiManager.linesPerFrame"],
                    frame_rate=group["acquisition_framerate_Hz"],
                    # scanfield_z=plane["scanimage_scanfield_z"],
                    # scanfield_z_unit=SizeUnit.UM,
                    # power=plane["scanimage_power"],
                )
                fovs.append(fov)
        data_streams.append(
            Stream(
                camera_names=["Mesoscope"],
                stream_start_time=self.job_settings.session_start_time,
                stream_end_time=self.job_settings.session_end_time,
                ophys_fovs=fovs,
                stream_modalities=[Modality.POPHYS],
            )
        )
        for camera in extracted_source.keys():
            if camera != "platform":
                start_time = datetime.strptime(
                    extracted_source[camera]["RecordingReport"]["TimeStart"],
                    "%Y-%m-%dT%H:%M:%SZ",
                )
                end_time = datetime.strptime(
                    extracted_source[camera]["RecordingReport"]["TimeEnd"],
                    "%Y-%m-%dT%H:%M:%SZ",
                )
                camera_name = camera.split("_")[1]
                data_streams.append(
                    Stream(
                        camera_names=[camera_name],
                        stream_start_time=start_time,
                        stream_end_time=end_time,
                        stream_modalities=[Modality.BEHAVIOR_VIDEOS],
                    )
                )
        vasculature_fp = next(
            self.job_settings.input_source.glob("*vasculature*.tif"), ""
        )
        # Pull datetime from vasculature.
        # Derived from
        # https://stackoverflow.com/questions/46477712/
        #   reading-tiff-image-metadata-in-python
        with Image.open(vasculature_fp) as img:
            vasculature_dt = [
                img.tag[key]
                for key in img.tag.keys()
                if "DateTime" in TAGS[key]
            ][0]
        vasculature_dt = datetime.strptime(
            vasculature_dt[0], "%Y:%m:%d %H:%M:%S"
        )
        data_streams.append(
            Stream(
                camera_names=["Vasculature"],
                stream_start_time=vasculature_dt,
                stream_end_time=vasculature_dt,
                stream_modalities=[
                    Modality.CONFOCAL
                ],  # TODO: ask Saskia about this
            )
        )
        return Session(
            experimenter_full_name=self.job_settings.experimenter_full_name,
            session_type="Mesoscope",
            subject_id=self.job_settings.subject_id,
            iacuc_protocol=self.job_settings.iacuc_protocol,
            session_start_time=self.job_settings.session_start_time,
            session_end_time=self.job_settings.session_end_time,
            rig_id=extracted_source["platform"]["rig_id"],
            data_streams=data_streams,
            mouse_platform_name=self.job_settings.mouse_platform_name,
            active_mouse_platform=True,
        )

    def run_job(self) -> None:
        """
        Run the etl job
        Returns
        -------
        None
        """
        extracted = self._extract()
        transformed = self._transform(extracted_source=extracted)
        transformed.write_standard_file(
            output_directory=self.job_settings.output_directory
        )

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


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    metl = MesoscopeEtl.from_args(sys_args)
    metl.run_job()
