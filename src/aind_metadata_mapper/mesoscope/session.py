"""Mesoscope ETL"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Union, Tuple

import h5py as h5
import tifffile
from aind_data_schema.core.session import (
    FieldOfView,
    LaserConfig,
    Session,
    Stream,
)
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import SizeUnit

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.mesoscope.models import JobSettings
from aind_metadata_mapper.stimulus.camstim import Camstim, CamstimSettings


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

    # TODO: Deprecate this constructor. Use GenericEtl constructor instead
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
        if isinstance(job_settings_model.behavior_source, str):
            job_settings_model.behavior_source = Path(
                job_settings_model.behavior_source
            )
        camstim_output = job_settings_model.output_directory
        if job_settings_model.make_camsitm_dir:
            camstim_output = (
                job_settings_model.output_directory
                / f"{job_settings_model.session_id}_behavior"
            )
        super().__init__(job_settings=job_settings_model)
        camstim_settings = CamstimSettings(
            input_source=self.job_settings.input_source,
            output_directory=camstim_output,
            session_id=self.job_settings.session_id,
            subject_id=self.job_settings.subject_id,
        )
        self.camstim = Camstim(camstim_settings)

    @staticmethod
    def _read_metadata(tiff_path: Path):
        """
        Calls tifffile.read_scanimage_metadata on the specified
        path and returns teh result. This method was factored
        out so that it could be easily mocked in unit tests.
        """

        with open(tiff_path, "rb") as tiff:
            file_handle = tifffile.FileHandle(tiff)
            file_contents = tifffile.read_scanimage_metadata(file_handle)
        return file_contents

    def _read_h5_metadata(self, h5_path: str):
        """Reads scanimage metadata from h5path

        Parameters
        ----------
        h5_path : str
            Path to h5 file

        Returns
        -------
        dict
        """
        data = h5.File(h5_path)
        try:
            file_contents = data["scanimage_metadata"][()].decode()
        except KeyError:
            file_contents = '[{"SI.hRoiManager.pixelsPerLine": 512, "SI.hRoiManager.linesPerFrame": 512}]'  # noqa
        data.close()
        file_contents = json.loads(file_contents)
        return file_contents

    def _extract_behavior_metdata(self) -> dict:
        """Loads behavior metadata from the behavior json files
        Returns
        -------
        dict
            behavior video metadata
        """
        session_metadata = {}
        session_id = self.job_settings.session_id
        for ftype in sorted(
            list(self.job_settings.behavior_source.glob("*json"))
        ):
            if (
                ("Behavior" in ftype.stem and session_id in ftype.stem)
                or ("Eye" in ftype.stem and session_id in ftype.stem)
                or ("Face" in ftype.stem and session_id in ftype.stem)
            ):
                with open(ftype, "r") as f:
                    session_metadata[ftype.stem] = json.load(f)
        return session_metadata

    def _extract_platform_metadata(self, session_metadata: dict) -> dict:
        """Parses the platform json file and returns the metadata

        Parameters
        ----------
        session_metadata : dict
            For session parsing

        Returns
        -------
        dict
            _description_
        """
        input_source = next(
            self.job_settings.input_source.glob("*platform.json"), ""
        )
        if (
            isinstance(input_source, str) and input_source == ""
        ) or not input_source.exists():
            raise ValueError("No platform json file found in directory")
        with open(input_source, "r") as f:
            session_metadata["platform"] = json.load(f)

        return session_metadata

    def _extract_time_series_metadata(self) -> dict:
        """Grab time series metadata from TIFF or HDF5

        Returns
        -------
        dict
            timeseries metadata
        """
        timeseries = next(
            self.job_settings.input_source.glob("*timeseries*.tiff"), ""
        )
        if timeseries:
            meta = self._read_metadata(timeseries)
        else:
            experiment_dir = list(
                self.job_settings.input_source.glob("ophys_experiment*")
            )[0]
            experiment_id = experiment_dir.name.split("_")[-1]
            timeseries = next(experiment_dir.glob(f"{experiment_id}.h5"))
            meta = self._read_h5_metadata(str(timeseries))

        return meta

    def _extract(self) -> dict:
        """extract data from the platform json file and tiff file (in the
        future).
        If input source is a file, will extract the data from the file.
        The input source is a directory, will extract the data from the
        directory.

        Returns
        -------
        (dict, dict)
            The extracted data from the platform json file and the time series
        """
        # The pydantic models will validate that the user inputs a Path.
        # We can add validators there if we want to coerce strings to Paths.
        session_metadata = self._extract_behavior_metdata()
        session_metadata = self._extract_platform_metadata(session_metadata)
        meta = self._extract_time_series_metadata()
        return session_metadata, meta

    def _camstim_epoch_and_session(self) -> Tuple[list, str]:
        """Get the camstim table and epochs

        Returnsd
        -------
        list
            The camstim table and epochs
        """
        if self.camstim.behavior:
            self.camstim.build_behavior_table()
        else:
            self.camstim.build_stimulus_table(modality="ophys")
        return self.camstim.epochs_from_stim_table(), self.camstim.session_type

    def _transform(self, extracted_source: dict, meta: dict) -> Session:
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
        fovs = []
        count = 0
        for group in imaging_plane_groups:
            power_ratio = group.get("scanimage_split_percent", None)
            if power_ratio:
                power_ratio = float(power_ratio)
            for plane in group["imaging_planes"]:
                fov = FieldOfView(
                    coupled_fov_index=int(
                        group["local_z_stack_tif"].split(".")[0][-1]
                    ),
                    index=count,
                    fov_coordinate_ml=self.job_settings.fov_coordinate_ml,
                    fov_coordinate_ap=self.job_settings.fov_coordinate_ap,
                    fov_reference=self.job_settings.fov_reference,
                    magnification=self.job_settings.magnification,
                    fov_scale_factor=0.78,
                    imaging_depth=plane["targeted_depth"],
                    targeted_structure=self._STRUCTURE_LOOKUP_DICT[
                        plane["targeted_structure_id"]
                    ],
                    scanimage_roi_index=plane["scanimage_roi_index"],
                    fov_width=meta[0]["SI.hRoiManager.pixelsPerLine"],
                    fov_height=meta[0]["SI.hRoiManager.linesPerFrame"],
                    frame_rate=group["acquisition_framerate_Hz"],
                    scanfield_z=plane["scanimage_scanfield_z"],
                    power=(
                        float(plane.get("scanimage_power", ""))
                        if not group.get("scanimage_power_percent", "")
                        else float(group.get("scanimage_power_percent", ""))
                    ),
                    power_ratio=power_ratio,
                )
                count += 1
                fovs.append(fov)
        data_streams = [
            Stream(
                light_sources=[
                    LaserConfig(
                        device_type="Laser",
                        name="Laser",
                        wavelength=920,
                        wavelength_unit=SizeUnit.NM,
                    ),
                ],
                stream_start_time=self.job_settings.session_start_time,
                stream_end_time=self.job_settings.session_end_time,
                ophys_fovs=fovs,
                stream_modalities=[Modality.POPHYS],
                camera_names=[
                    "Mesoscope",
                    "Behavior",
                    "Eye",
                    "Face",
                ],
            )
        ]
        stim_epochs, session_type = self._camstim_epoch_and_session()
        return Session(
            experimenter_full_name=self.job_settings.experimenter_full_name,
            session_type=session_type,
            subject_id=self.job_settings.subject_id,
            iacuc_protocol=self.job_settings.iacuc_protocol,
            session_start_time=self.job_settings.session_start_time,
            session_end_time=self.job_settings.session_end_time,
            rig_id=extracted_source["platform"]["rig_id"],
            data_streams=data_streams,
            stimulus_epochs=stim_epochs,
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
        session_meta, movie_meta = self._extract()
        transformed = self._transform(
            extracted_source=session_meta, meta=movie_meta
        )
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

        logging.warning(
            "This method will be removed in future versions. "
            "Please use JobSettings.from_args instead."
        )

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
    main_job_settings = JobSettings.from_args(sys_args)
    metl = MesoscopeEtl(job_settings=main_job_settings)
    metl.run_job()
