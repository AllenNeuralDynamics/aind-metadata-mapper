"""Mesocope ETL derived from Bergamo session module"""
from typing import List, Tuple, Union
import json
from pathlib import Path
from datetime import datetime
import argparse
import sys
from pydantic import Field
from pydantic_settings import BaseSettings
import tifffile
import copy
import numpy as np
from PIL import Image
from PIL.TiffTags import TAGS

from aind_metadata_mapper.core import BaseEtl
from aind_data_schema.models.units import PowerUnit, SizeUnit
from aind_data_schema.models.modalities import Modality
from aind_data_schema.core.session import (
    FieldOfView,
    Stream,
    Session,
)


structure_lookup_dict = {
    385: "VISp",
    394: "VISam",
    402: "VISal",
    409: "VISl",
    417: "VISrl",
    533: "VISpm",
    312782574: "VISli",
}

video_files = ["Eye", "Face", "Behavior"]


def _read_metadata(tiff_path: Path):
    """
    Calls tifffile.read_scanimage_metadata on the specified
    path and returns teh result. This method was factored
    out so that it could be easily mocked in unit tests.
    """
    return tifffile.read_scanimage_metadata(open(tiff_path, "rb"))


class ScanImageMetadata(object):
    """
    A class to handle reading and parsing the metadata that
    comes with the TIFF files produced by ScanImage

    Parameters
    ----------
    tiff_path: Path
        Path to the TIFF file whose metadata we are parsing
    """

    def __init__(self, tiff_path: Path):
        self._file_path = tiff_path
        if not tiff_path.is_file():
            raise ValueError(f"{tiff_path.resolve().absolute()} " "is not a file")
        self._metadata = _read_metadata(tiff_path)

    @property
    def file_path(self) -> Path:
        return self._file_path

    @property
    def raw_metadata(self) -> tuple:
        """
        Return a copy of the raw metadata as read by
        tifffile.read_scanimage_metadata.
        """
        return copy.deepcopy(self._metadata)

    @property
    def numVolumes(self) -> int:
        """
        The metadata field representing the number of volumes
        recorded by the rig
        """
        if not hasattr(self, "_numVolumes"):
            value = self._metadata[0]["SI.hStackManager.actualNumVolumes"]
            if not isinstance(value, int):
                raise ValueError(
                    f"in {self._file_path}\n"
                    "SI.hStackManager.actualNumVolumes is a "
                    f"{type(value)}; expected int"
                )

            self._numVolumes = value

        return self._numVolumes

    @property
    def numSlices(self) -> int:
        """
        The metadata field representing the number of slices
        recorded by the rig
        """
        if not hasattr(self, "_numSlices"):
            value = self._metadata[0]["SI.hStackManager.actualNumSlices"]
            if not isinstance(value, int):
                raise ValueError(
                    f"in {self._file_path}\n"
                    "SI.hStackManager.actualNumSlices is a "
                    f"{type(value)}; expected int"
                )
            self._numSlices = value

        return self._numSlices

    @property
    def channelSave(self) -> Union[int, List[int]]:
        """
        The metadata field representing which channels were saved
        in this TIFF. Either 1 or [1, 2]
        """
        if not hasattr(self, "_channelSave"):
            self._channelSave = self._metadata[0]["SI.hChannels.channelSave"]
        return self._channelSave

    @property
    def defined_rois(self) -> List[dict]:
        """
        Get the ROIs defined in this TIFF file

        This is list of dicts, each dict containing the ScanImage
        metadata for a given ROI

        In this context, an ROI is a 3-dimensional volume of the brain
        that was scanned by the microscope.
        """
        if not hasattr(self, "_defined_rois"):
            roi_parent = self._metadata[1]["RoiGroups"]
            roi_group = roi_parent["imagingRoiGroup"]["rois"]
            if isinstance(roi_group, dict):
                self._defined_rois = [
                    roi_group,
                ]
            elif isinstance(roi_group, list):
                self._defined_rois = roi_group
            else:
                msg = "unable to parse "
                msg += "self._metadata[1]['RoiGroups']"
                msg += "['imagingROIGroup']['rois'] "
                msg += f"of type {type(roi_group)}"
                raise RuntimeError(msg)

        # use copy to make absolutely sure self._defined_rois
        # is not accidentally changed downstream
        return copy.deepcopy(self._defined_rois)

    @property
    def n_rois(self) -> int:
        """
        Number of ROIs defined in the metadata for this TIFF file.
        """
        if not hasattr(self, "_n_rois"):
            self._n_rois = len(self.defined_rois)
        return self._n_rois

    @property
    def fov_width(self) -> int:
        """FOV width from the pixelsPerLine key

        Returns
        -------
        fov_width: int
            The width of the field of view in pixels
        """
        return self._metadata[0]["SI.hRoiManager.pixelsPerLine"]

    @property
    def fov_height(self) -> int:
        """FOV height from the linesPerFrame key

        Returns
        -------
        fov_height: int
            The height of the field of view in pixels
        """
        return self._metadata[0]["SI.hRoiManager.linesPerFrame"]

    @property
    def fov_scale_factor(self) -> float:
        """The scale factor of the field of view

        Returns
        -------
        fov_scale_factor: float
            The scale factor of the field of view
        """
        return self._metadata[0]["SI.hRoiManager.scanZoomFactor"]

    def zs_for_roi(self, i_roi: int) -> List[int]:
        """
        Return a list of the z-values at which the specified
        ROI was scanned
        """
        if i_roi >= self.n_rois:
            msg = f"You asked for ROI {i_roi}; "
            msg += f"there are only {self.n_rois} "
            msg += "specified in this TIFF file"
            raise ValueError(msg)
        return self.defined_rois[i_roi]["zs"]

    def all_zs(self) -> List:
        """
        Return the structure that lists the z-values of all scans divided
        into imaging groups, i.e.

        scanimage_metadata[0]['SI.hStackManager.zsAllActuators']

        (in historical versions of ScanImage, the desired key is actually
        'SI.hStackManager.zs'; this method will try that if
        'zsAllActuators' is not present)
        """
        key_to_use = "SI.hStackManager.zsAllActuators"
        if key_to_use in self._metadata[0]:
            return self._metadata[0][key_to_use]

        other_key = "SI.hStackManager.zs"
        if other_key not in self._metadata[0]:
            msg = "Cannot load all_zs from "
            msg += f"{self._file_path.resolve().absolute()}\n"
            msg += f"Neither {key_to_use} nor "
            msg += f"{other_key} present"
            raise ValueError(msg)

        return self._metadata[0][other_key]

    def roi_center(self, i_roi: int, atol: float = 1.0e-5) -> Tuple[float, float]:
        """
        Return the X, Y center of the specified ROI.

        If the scanfields within an ROI have inconsistent values to within
        absolute tolerance atol, raise an error (this is probably allowed
        by ScanImage; I do not think we are ready to handle it, yet).

        Parameters
        ----------
        i_roi: int

        atol: float
            The tolerance in X and Y within which two
            points in (X, Y) space are allowed to be the same

        Returns
        -------
        center: Tuple[float, float]
           (X_coord, Y_coord)
        """
        if i_roi >= self.n_rois:
            msg = f"You asked for ROI {i_roi}; "
            msg += f"there are only {self.n_rois} "
            msg += "specified in {self._file_path.resolve().absolute()}"
            raise ValueError(msg)

        scanfields = self.defined_rois[i_roi]["scanfields"]
        if isinstance(scanfields, dict):
            scanfields = [scanfields]
        elif not isinstance(scanfields, list):
            msg = "Expected scanfields to be either a list "
            msg += f"or a dict; instead got {type(scanfields)}"
            raise RuntimeError(msg)
        avg_x = 0.0
        avg_y = 0.0
        for field in scanfields:
            center = field["centerXY"]
            avg_x += center[0]
            avg_y += center[1]
        avg_x = avg_x / len(scanfields)
        avg_y = avg_y / len(scanfields)

        is_valid = True
        for field in scanfields:
            center = field["centerXY"]
            if abs(center[0] - avg_x) > atol:
                is_valid = False
            if abs(center[1] - avg_y) > atol:
                is_valid = False

        if not is_valid:
            msg = "\nInconsistent scanfield centers:\n"
            for field in scanfields:
                msg += "{field['centerXY']}\n"
            raise RuntimeError(msg)

        return (avg_x, avg_y)

    def roi_size(self, i_roi: int) -> Tuple[float, float]:
        """
        Return the size in physical units of an ROI. Will raise an error
        if the ROI has multiple scanfields with inconsistent size values.

        Parameters
        ----------
        i_roi: int:
            Index of the ROI whose size is to be returned.

        Returns
        -------
        sizexy: Tuple[float, float]
            This is just the 'sizeXY' element associated with an ROI's
            scanfield metadata.
        """
        if i_roi >= self.n_rois:
            msg = f"You asked for ROI {i_roi}; "
            msg += f"there are only {self.n_rois} "
            msg += "specified in {self._file_path.resolve().absolute()}"
            raise ValueError(msg)

        scanfields = self.defined_rois[i_roi]["scanfields"]
        if isinstance(scanfields, dict):
            scanfields = [scanfields]
        elif not isinstance(scanfields, list):
            msg = "Expected scanfields to be either a list "
            msg += f"or a dict; instead got {type(scanfields)}"
            raise RuntimeError(msg)

        size_x = None
        size_y = None
        for this_scanfield in scanfields:
            if size_x is None:
                size_x = this_scanfield["sizeXY"][0]
                size_y = this_scanfield["sizeXY"][1]
            else:
                same_x = np.allclose(size_x, this_scanfield["sizeXY"][0])
                same_y = np.allclose(size_y, this_scanfield["sizeXY"][1])
                if not same_x or not same_y:
                    msg = f"{self._file_path.resolve().absolute()}\n"
                    msg += f"i_roi: {i_roi}\n"
                    msg += "has multiple scanfields with differing sizeXY\n"
                    msg += "asking for roi_size is meaningless"
                    raise ValueError(msg)

        if size_x is None or size_y is None:
            raise ValueError(
                "Could not find sizeXY for "
                f"ROI {i_roi} in {self._file_path.resolve().absolute()}"
            )

        return (size_x, size_y)

    def roi_resolution(self, i_roi: int) -> Tuple[int, int]:
        """
        Return the size in pixels of an ROI. Will raise an error
        if the ROI has multiple scanfields with inconsistent values.

        Parameters
        ----------
        i_roi: int:
            Index of the ROI whose size is to be returned.

        Returns
        -------
        resolutionxy: Tuple[int, int]
            This is just the 'pixelResolutionXY' element associated with
            an ROI's scanfield metadata.
        """
        if i_roi >= self.n_rois:
            msg = f"You asked for ROI {i_roi}; "
            msg += f"there are only {self.n_rois} "
            msg += "specified in {self._file_path.resolve().absolute()}"
            raise ValueError(msg)

        scanfields = self.defined_rois[i_roi]["scanfields"]
        if isinstance(scanfields, dict):
            scanfields = [scanfields]
        elif not isinstance(scanfields, list):
            msg = "Expected scanfields to be either a list "
            msg += f"or a dict; instead got {type(scanfields)}"
            raise RuntimeError(msg)

        pix_x = None
        pix_y = None
        for this_scanfield in scanfields:
            if pix_x is None:
                pix_x = this_scanfield["pixelResolutionXY"][0]
                pix_y = this_scanfield["pixelResolutionXY"][1]
            else:
                same_x = pix_x == this_scanfield["pixelResolutionXY"][0]
                same_y = pix_y == this_scanfield["pixelResolutionXY"][1]
                if not same_x or not same_y:
                    msg = f"{self._file_path.resolve().absolute()}\n"
                    msg += f"i_roi: {i_roi}\n"
                    msg += "has multiple scanfields with differing "
                    msg += "pixelResolutionXY\n"
                    msg += "asking for roi_size is meaningless"
                    raise ValueError(msg)

        if pix_x is None or pix_y is None:
            raise ValueError(
                "Could not find pixelResolutionXY for "
                f"ROI {i_roi} in {self._file_path.resolve().absolute()}"
            )

        return (pix_x, pix_y)


class UserSettings(BaseSettings):
    """Data to be entered by the user. Modeled after BergamoEtl class"""

    # TODO: for now this will need to be directly input by the user. In the future, the worfklow sequencing engine should be able to put this in a json or we can extract it from SLIMS
    session_start_time: datetime
    session_end_time: datetime
    subject_id: str
    project: str
    iacuc_protocol: str = "2115"
    magnification: str = "16x"
    fov_coordinate_ml: float = 1.5
    fov_coordinate_ap: float = 1.5
    fov_reference: str = "Bregma"
    experimenter_full_name: List[str] = Field(..., title="Full name of the experimenter")


class MesoscopeEtl(BaseEtl):
    """Class to manage transforming mesoscope platform json and metadata into a session object.
    Modeled after BergamoEtl class"""

    def __init__(
        self,
        input_source: Union[Path, str],
        behavior_source: Union[Path, str],
        output_directory: Path,
        user_settings: UserSettings,
    ):
        super().__init__(input_source, output_directory)
        self.input_source = input_source
        self.behavior_source = behavior_source
        self.output_directory = output_directory
        self.user_settings = user_settings

    def _extract(self) -> dict:
        """extract data from the platform json file and tiff file (in the future).
        If input source is a file, will extract the data from the file.
        The input source is a directory, will extract the data from the directory.

        Returns
        -------
        dict
            The extracted data from the platform json file.
        """
        if isinstance(self.input_source, str):
            input_source = Path(self.input_source)
        else:
            input_source = self.input_source
        if isinstance(self.behavior_source, str):
            behavior_source = Path(self.behavior_source)
        else:
            behavior_source = self.behavior_source
        session_metadata = {}
        if behavior_source.is_dir():
            for ftype in behavior_source.glob("*json"):
                if "Behavior" in ftype.stem or "Eye" in ftype.stem or "Face" in ftype.stem:
                    with open(ftype, "r") as f:
                        session_metadata[ftype.stem] = json.load(f)
        else:
            raise ValueError("Behavior source must be a directory")
        if input_source.is_dir():
            input_source = next(input_source.glob("*platform.json", ""))
            if not input_source.exists():
                raise ValueError("No platform json file found in directory")
        with open(input_source, "r") as f:
            session_metadata["platform"] = json.load(f)
        return session_metadata

    def _transform(self, session_data: dict) -> dict:
        """Transform the platform data into a session object

        Parameters
        ----------
        session_data : dict
            Extracted data from the camera jsons and platform json.
        user_settins: UserSettings
            The user settings for the session
        Returns
        -------
        Session
            The session object
        """
        imaging_plane_groups = session_data["platform"]["imaging_plane_groups"]
        timeseries = next(self.input_source.glob("*timeseries*.tif"), "")
        meta = ScanImageMetadata(timeseries)
        fovs = []
        data_streams = []
        for group in imaging_plane_groups:
            for plane in group["imaging_planes"]:
                fov = FieldOfView(
                    index=int(group["local_z_stack_tif"].split(".")[0][-1]),
                    fov_coordinate_ml=self.user_settings.fov_coordinate_ml,
                    fov_coordinate_ap=self.user_settings.fov_coordinate_ap,
                    fov_reference=self.user_settings.fov_reference,
                    magnification=self.user_settings.magnification,
                    fov_scale_factor=meta.fov_scale_factor,
                    imaging_depth=plane["targeted_depth"],
                    targeted_structure=structure_lookup_dict[plane["targeted_structure_id"]],
                    fov_width=meta.fov_width,
                    fov_height=meta.fov_height,
                    frame_rate=group["acquisition_framerate_Hz"],
                    scanfield_z=plane["scanimage_scanfield_z"],
                    power=plane["scanimage_power"],
                )
                fovs.append(fov)
        data_streams.append(
            Stream(
                camera_names=["Mesoscope"],
                stream_start_time=self.user_settings.session_start_time,
                stream_end_time=self.user_settings.session_end_time,
                ophys_fovs=fovs,
                mouse_platform_name="some mouse platform",
                active_mouse_platform=True,
                stream_modalities=[Modality.POPHYS],
            )
        )
        for camera in session_data.keys():
            if camera != "platform":
                start_time = datetime.strptime(
                    session_data[camera]["RecordingReport"]["TimeStart"], "%Y-%m-%dT%H:%M:%SZ"
                )
                end_time = datetime.strptime(
                    session_data[camera]["RecordingReport"]["TimeEnd"], "%Y-%m-%dT%H:%M:%SZ"
                )
                camera_name = camera.split("_")[1]
                data_streams.append(
                    Stream(
                        camera_names=[camera_name],
                        stream_start_time=start_time,
                        stream_end_time=end_time,
                        mouse_platform_name="some mouse platform",
                        active_mouse_platform=True,
                        stream_modalities=[Modality.BEHAVIOR_VIDEOS],
                    )
                )
        vasculature_fp = next(self.input_source.glob("*vasculature*.tif"), "")
        # Pull datetime from vasculature. Derived from https://stackoverflow.com/questions/46477712/reading-tiff-image-metadata-in-python
        with Image.open(vasculature_fp) as img:
            vasculature_dt = [img.tag[key] for key in img.tag.keys() if "DateTime" in TAGS[key]][0]
        vasculature_dt = datetime.strptime(vasculature_dt[0], "%Y:%m:%d %H:%M:%S")
        data_streams.append(
            Stream(
                camera_names=["Vasculature"],
                stream_start_time=vasculature_dt,
                stream_end_time=vasculature_dt,
                mouse_platform_name="some mouse platform",
                active_mouse_platform=True,
                stream_modalities=[Modality.CONFOCAL],  # TODO: ask Saskia about this
            )
        )
        return Session(
            experimenter_full_name=self.user_settings.experimenter_full_name,
            session_type="Mesoscope",
            subject_id=self.user_settings.subject_id,
            # project=self.user_settings.project,
            iacuc_protocol=self.user_settings.iacuc_protocol,
            session_start_time=self.user_settings.session_start_time,
            session_end_time=self.user_settings.session_end_time,
            rig_id=session_data["platform"]["rig_id"],
            data_streams=data_streams,
        )

    def run_job(self):
        pass

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
            "-i",
            "--input-source",
            required=False,
            type=str,
            help="Directory where acquisition files are located",
        )
        parser.add_argument(
            "-i",
            "--behavior-source",
            required=False,
            type=str,
            help="Directory where behavior files are located",
        )
        parser.add_argument(
            "-o",
            "--output-directory",
            required=False,
            default=".",
            type=str,
            help=("Directory to save json file to. Defaults to current working " "directory."),
        )
        parser.add_argument(
            "-u",
            "--user-settings",
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
        user_settings_from_args = UserSettings(**job_args.user_settings)
        return cls(
            input_source=Path(job_args.input_source),
            output_directory=Path(job_args.output_directory),
            behavior_source=Path(job_args.behavior_source),
            user_settings=user_settings_from_args,
        )


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    metl = MesoscopeEtl.from_args(sys_args)
    metl.run_job()
