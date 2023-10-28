"""Module to map bergamo metadata into a session model"""

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, time
from os import PathLike
from pathlib import Path
from typing import Dict, List, Tuple, Union

import numpy as np
from aind_data_schema.session import (
    Detector,
    FieldOfView,
    Laser,
    Modality,
    Session,
    Stream,
)
from aind_data_schema.stimulus import (
    PhotoStimulation,
    PhotoStimulationGroup,
    StimulusEpoch,
)
from aind_data_schema.utils.units import PowerUnit, SizeUnit
from pydantic import BaseSettings, Extra
from ScanImageTiffReader import ScanImageTiffReader

from aind_metadata_mapper.core import BaseEtl


class UserSettings(BaseSettings):
    """Data that needs to be input by user. Can be pulled from env vars with
    BERGAMO prefix or set explicitly."""

    experimenter_full_name: List[str]
    subject_id: str
    # TODO: Look into if the following can be extracted from tif directory
    session_start_time: datetime
    session_end_time: datetime
    stream_start_time: datetime
    stream_end_time: datetime
    stimulus_start_time: time
    stimulus_end_time: time

    # Data that might change but can have default values
    session_type: str = "BCI"
    iacuc_protocol: str = "2115"
    rig_id: str = "Bergamo photostim."
    camera_names: Tuple[str] = ("Side Camera",)
    laser_a_name: str = "Laser A"
    laser_a_wavelength: int = 920
    laser_a_wavelength_unit: SizeUnit = SizeUnit.NM
    detector_a_name: str = "PMT A"
    detector_a_exposure_time: float = 0.1
    detector_a_trigger_type: str = "Internal"
    fov_0_index: int = 0
    fov_0_imaging_depth: int = 150
    fov_0_targeted_structure: str = "M1"
    fov_0_coordinate_ml: float = 1.5
    fov_0_coordinate_ap: float = 1.5
    fov_0_reference: str = "Bregma"
    fov_0_magnification: str = "16x"
    photo_stim_inter_trial_interval: int = 10
    photo_stim_groups: List[Dict[str, int]] = [
        {"group_index": 0, "number_trials": 5},
        {"group_index": 0, "number_trials": 5},
    ]

    @property
    def num_of_photo_stim_groups(self):
        """Compute number of photo stimulation groups from list of groups"""
        return len(self.photo_stim_groups)

    class Config:
        """Config to set env var prefix to BERGAMO"""

        extra = Extra.forbid
        env_prefix = "BERGAMO_"


@dataclass(frozen=True)
class RawImageInfo:
    """Metadata from tif files"""

    metadata: str
    description0: str
    shape: List[int]


@dataclass(frozen=True)
class ParsedMetadata:
    """RawImageInfo gets parsed into this data"""

    metadata: dict
    roi_data: dict
    roi_metadata: dict
    frame_rate: str
    num_planes: int
    shape: List[int]
    description_first_frame: dict
    movie_start_time: datetime


class BergamoEtl(BaseEtl):
    """Class to manage transforming bergamo data files into a Session object"""

    def __init__(
        self,
        input_source: Union[str, PathLike],
        output_directory: Path,
        user_settings: UserSettings,
    ):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        input_source : Union[str, PathLike]
          Can be a string or a Path
        output_directory : Path
          The directory where to save the json files.
        user_settings: UserSettings
          Variables for a particular session
        """
        super().__init__(input_source, output_directory)
        self.user_settings = user_settings

    @staticmethod
    def _flat_dict_to_nested(flat: dict, key_delim: str = ".") -> dict:
        """
        Utility method to convert a flat dictionary into a nested dictionary.
        Modified from https://stackoverflow.com/a/50607551
        Parameters
        ----------
        flat : dict
          Example {"a.b.c": 1, "a.b.d": 2, "e.f": 3}
        key_delim : str
          Delimiter on dictionary keys. Default is '.'.

        Returns
        -------
        dict
          A nested dictionary like {"a": {"b": {"c":1, "d":2}, "e": {"f":3}}
        """

        def __nest_dict_rec(k, v, out) -> None:
            """Simple recursive method being called."""
            k, *rest = k.split(key_delim, 1)
            if rest:
                __nest_dict_rec(rest[0], v, out.setdefault(k, {}))
            else:
                out[k] = v

        result = {}
        for flat_key, flat_val in flat.items():
            __nest_dict_rec(flat_key, flat_val, result)
        return result

    def _parse_raw_image_info(
        self, raw_image_info: RawImageInfo
    ) -> ParsedMetadata:
        """
        Parses metadata from raw image info.
        Parameters
        ----------
        raw_image_info : RawImageInfo

        Returns
        -------
        ParsedMetadata
        """

        # The metadata contains two parts separated by \n\n. The top part
        # looks like
        # 'SI.abc.def = 1\n SI.abc.ghf=2'
        # We'll convert that to a nested dict.
        metadata_first_part = raw_image_info.metadata.split("\n\n")[0]
        flat_metadata_header_dict = dict(
            [
                (s.split(" = ", 1)[0], s.split(" = ", 1)[1])
                for s in metadata_first_part.split("\n")
            ]
        )
        metadata = self._flat_dict_to_nested(flat_metadata_header_dict)
        # Move SI dictionary up one level
        if "SI" in metadata.keys():
            si_contents = metadata.pop("SI")
            metadata.update(si_contents)

        # The second part is a standard json string. We'll extract it and
        # append it to our dictionary
        metadata_json = json.loads(raw_image_info.metadata.split("\n\n")[1])
        metadata["json"] = metadata_json

        # Convert description string to a dictionary
        first_frame_description_str = raw_image_info.description0.strip()
        description_first_image_dict = dict(
            [
                (s.split(" = ", 1)[0], s.split(" = ", 1)[1])
                for s in first_frame_description_str.split("\n")
            ]
        )
        frame_rate = metadata["hRoiManager"]["scanVolumeRate"]
        # TODO: Use .get instead of try/except and add coverage test
        try:
            z_collection = metadata["hFastZ"]["userZs"]
            num_planes = len(z_collection)  # pragma: no cover
        except Exception as e:  # new scanimage version
            logging.error(
                f"Multiple planes not handled in metadata collection. "
                f"HANDLE ME!!!: {repr(e)}"
            )
            # TODO: Check if this if/else is necessary
            if metadata["hFastZ"]["enable"] == "true":
                num_planes = 1  # pragma: no cover
            else:
                num_planes = 1

        roi_metadata = metadata["json"]["RoiGroups"]["imagingRoiGroup"]["rois"]

        if isinstance(roi_metadata, dict):
            roi_metadata = [roi_metadata]
        num_rois = len(roi_metadata)
        roi = {}
        w_px = []
        h_px = []
        cXY = []
        szXY = []
        for r in range(num_rois):
            roi[r] = {}
            roi[r]["w_px"] = roi_metadata[r]["scanfields"][
                "pixelResolutionXY"
            ][0]
            w_px.append(roi[r]["w_px"])
            roi[r]["h_px"] = roi_metadata[r]["scanfields"][
                "pixelResolutionXY"
            ][1]
            h_px.append(roi[r]["h_px"])
            roi[r]["center"] = roi_metadata[r]["scanfields"]["centerXY"]
            cXY.append(roi[r]["center"])
            roi[r]["size"] = roi_metadata[r]["scanfields"]["sizeXY"]
            szXY.append(roi[r]["size"])

        w_px = np.asarray(w_px)
        h_px = np.asarray(h_px)
        szXY = np.asarray(szXY)
        cXY = np.asarray(cXY)
        cXY = cXY - szXY / 2
        cXY = cXY - np.amin(cXY, axis=0)
        mu = np.median(np.transpose(np.asarray([w_px, h_px])) / szXY, axis=0)
        imin = cXY * mu

        n_rows_sum = np.sum(h_px)
        n_flyback = (raw_image_info.shape[1] - n_rows_sum) / np.max(
            [1, num_rois - 1]
        )

        irow = np.insert(np.cumsum(np.transpose(h_px) + n_flyback), 0, 0)
        irow = np.delete(irow, -1)
        irow = np.vstack((irow, irow + np.transpose(h_px)))

        data = {"fs": frame_rate, "nplanes": num_planes, "nrois": num_rois}
        if data["nrois"] == 1:
            data["mesoscan"] = 0
        else:
            # TODO: Add coverage example
            data["mesoscan"] = 1  # pragma: no cover
        # TODO: Add coverage example
        if data["mesoscan"]:  # pragma: no cover
            # data['nrois'] = num_rois #or irow.shape[1]?
            data["dx"] = []
            data["dy"] = []
            data["lines"] = []
            for i in range(num_rois):
                data["dx"] = np.hstack((data["dx"], imin[i, 1]))
                data["dy"] = np.hstack((data["dy"], imin[i, 0]))
                # TODO: NOT QUITE RIGHT YET
                data["lines"] = list(
                    range(
                        irow[0, i].astype("int32"),
                        irow[1, i].astype("int32") - 1,
                    )
                )
            data["dx"] = data["dx"].astype("int32")
            data["dy"] = data["dy"].astype("int32")
            logging.debug(f"data[dx]: {data['dx']}")
            logging.debug(f"data[dy]: {data['dy']}")
            logging.debug(f"data[lines]: {data['lines']}")
        movie_start_time = datetime.strptime(
            description_first_image_dict["epoch"], "[%Y %m %d %H %M %S.%f]"
        )

        return ParsedMetadata(
            metadata=metadata,
            roi_data=data,
            roi_metadata=roi_metadata,
            frame_rate=frame_rate,
            num_planes=num_planes,
            shape=raw_image_info.shape,
            description_first_frame=description_first_image_dict,
            movie_start_time=movie_start_time,
        )

    @staticmethod
    def _get_si_file_from_dir(
        source_dir: Path, regex_pattern: str = r"^.*?(\d+).tif+$"
    ) -> Path:
        """
        Utility method to scan top level of source_dir for .tif or .tiff files.
        Sorts them by file number and collects the first one. The directory
        contains files that look like neuron50_00001.tif, neuron50_00002.tif.
        Parameters
        ----------
        source_dir : Path
          Directory where the tif files are located
        regex_pattern : str
          Format of how files are expected to be organized. Default matches
          against something that ends with a series of digits and .tif(f)

        Returns
        -------
        Path
          File path of the first tif file.

        """
        compiled_regex = re.compile(regex_pattern)
        tif_filepath = None
        old_tif_number = None
        for root, dirs, files in os.walk(source_dir):
            for name in files:
                matched = re.match(compiled_regex, name)
                if matched:
                    tif_number = matched.group(1)
                    if old_tif_number is None or tif_number < old_tif_number:
                        old_tif_number = tif_number
                        tif_filepath = Path(os.path.join(root, name))

            # Only scan the top level files
            break
        if tif_filepath is None:
            raise FileNotFoundError("Directory must contain tif or tiff file!")
        else:
            return tif_filepath

    def _extract(self) -> RawImageInfo:
        """Extract metadata from bergamo session. If input source is a file,
        will extract data from file. If input source is a directory, will
        attempt to find a file."""
        if isinstance(self.input_source, str):
            input_source = Path(self.input_source)
        else:
            input_source = self.input_source

        if os.path.isfile(input_source):
            file_with_metadata = input_source
        else:
            file_with_metadata = self._get_si_file_from_dir(input_source)
        # Not sure if a custom header was appended, but we can't use
        # o=json.loads(reader.metadata()) directly
        with ScanImageTiffReader(str(file_with_metadata)) as reader:
            img_metadata = reader.metadata()
            img_description = reader.description(0)
            img_shape = reader.shape()
        return RawImageInfo(
            metadata=img_metadata,
            description0=img_description,
            shape=img_shape,
        )

    def _transform(self, extracted_source: RawImageInfo) -> Session:
        """
        Transforms the raw data extracted from the tif directory into a
        Session object.
        Parameters
        ----------
        extracted_source : RawImageInfo

        Returns
        -------
        Session

        """
        siHeader = self._parse_raw_image_info(extracted_source)
        photostim_groups = siHeader.metadata["json"]["RoiGroups"][
            "photostimRoiGroups"
        ]

        data_stream = Stream(
            stream_start_time=self.user_settings.stream_start_time,
            stream_end_time=self.user_settings.stream_end_time,
            stream_modalities=[Modality.POPHYS],
            camera_names=list(self.user_settings.camera_names),
            light_sources=[
                Laser(
                    name=self.user_settings.laser_a_name,
                    wavelength=self.user_settings.laser_a_wavelength,
                    wavelength_unit=self.user_settings.laser_a_wavelength_unit,
                    excitation_power=int(
                        siHeader.metadata["hBeams"]["powers"][1:-1].split()[0]
                    ),
                    excitation_power_unit=PowerUnit.PERCENT,
                ),
            ],
            detectors=[
                Detector(
                    name=self.user_settings.detector_a_name,
                    exposure_time=self.user_settings.detector_a_exposure_time,
                    trigger_type=self.user_settings.detector_a_trigger_type,
                ),
            ],
            ophys_fovs=[
                FieldOfView(
                    index=self.user_settings.fov_0_index,
                    imaging_depth=self.user_settings.fov_0_imaging_depth,
                    targeted_structure=(
                        self.user_settings.fov_0_targeted_structure
                    ),
                    fov_coordinate_ml=self.user_settings.fov_0_coordinate_ml,
                    fov_coordinate_ap=self.user_settings.fov_0_coordinate_ap,
                    fov_reference=self.user_settings.fov_0_reference,
                    fov_width=int(
                        siHeader.metadata["hRoiManager"]["pixelsPerLine"]
                    ),
                    fov_height=int(
                        siHeader.metadata["hRoiManager"]["linesPerFrame"]
                    ),
                    magnification=self.user_settings.fov_0_magnification,
                    fov_scale_factor=float(
                        siHeader.metadata["hRoiManager"]["scanZoomFactor"]
                    ),
                    frame_rate=float(
                        siHeader.metadata["hRoiManager"]["scanFrameRate"]
                    ),
                ),
            ],
        )
        return Session(
            experimenter_full_name=self.user_settings.experimenter_full_name,
            session_start_time=self.user_settings.session_start_time,
            session_end_time=self.user_settings.session_end_time,
            subject_id=self.user_settings.subject_id,
            session_type=self.user_settings.session_type,
            iacuc_protocol=self.user_settings.iacuc_protocol,
            rig_id=self.user_settings.rig_id,
            data_streams=[data_stream],
            stimulus_epochs=[
                StimulusEpoch(
                    stimulus=PhotoStimulation(
                        stimulus_name="PhotoStimulation",
                        number_groups=(
                            self.user_settings.num_of_photo_stim_groups
                        ),
                        groups=[
                            PhotoStimulationGroup(
                                group_index=(
                                    self.user_settings.photo_stim_groups[0][
                                        "group_index"
                                    ]
                                ),
                                number_of_neurons=int(
                                    np.array(
                                        photostim_groups[0]["rois"][1][
                                            "scanfields"
                                        ]["slmPattern"]
                                    ).shape[0]
                                ),
                                stimulation_laser_power=int(
                                    photostim_groups[0]["rois"][1][
                                        "scanfields"
                                    ]["powers"]
                                ),
                                number_trials=(
                                    self.user_settings.photo_stim_groups[0][
                                        "number_trials"
                                    ]
                                ),
                                number_spirals=int(
                                    photostim_groups[0]["rois"][1][
                                        "scanfields"
                                    ]["repetitions"]
                                ),
                                spiral_duration=photostim_groups[0]["rois"][1][
                                    "scanfields"
                                ]["duration"],
                                inter_spiral_interval=photostim_groups[0][
                                    "rois"
                                ][2]["scanfields"]["duration"],
                            ),
                            PhotoStimulationGroup(
                                group_index=(
                                    self.user_settings.photo_stim_groups[1][
                                        "group_index"
                                    ]
                                ),
                                number_of_neurons=int(
                                    np.array(
                                        photostim_groups[0]["rois"][1][
                                            "scanfields"
                                        ]["slmPattern"]
                                    ).shape[0]
                                ),
                                stimulation_laser_power=int(
                                    photostim_groups[0]["rois"][1][
                                        "scanfields"
                                    ]["powers"]
                                ),
                                number_trials=(
                                    self.user_settings.photo_stim_groups[1][
                                        "number_trials"
                                    ]
                                ),
                                number_spirals=int(
                                    photostim_groups[0]["rois"][1][
                                        "scanfields"
                                    ]["repetitions"]
                                ),
                                spiral_duration=photostim_groups[0]["rois"][1][
                                    "scanfields"
                                ]["duration"],
                                inter_spiral_interval=photostim_groups[0][
                                    "rois"
                                ][2]["scanfields"]["duration"],
                            ),
                        ],
                        inter_trial_interval=(
                            self.user_settings.photo_stim_inter_trial_interval
                        ),
                    ),
                    stimulus_start_time=(
                        self.user_settings.stimulus_start_time
                    ),
                    stimulus_end_time=self.user_settings.stimulus_end_time,
                )
            ],
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
            "-i",
            "--input-source",
            required=False,
            type=str,
            help="Directory where tif files are located",
        )
        parser.add_argument(
            "-o",
            "--output-directory",
            required=False,
            default=".",
            type=str,
            help=(
                "Directory to save json file to. Defaults to current working "
                "directory."
            ),
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
                 "stream_start_time": "2023-10-10T11:10:10",
                 "stream_end_time":"2023-10-10T17:10:10",
                 "stimulus_start_time":"12:10:10",
                 "stimulus_end_time":"13:10:10"}'
                """
            ),
        )
        job_args = parser.parse_args(args)
        user_settings_from_args = UserSettings(**job_args.user_settings)
        return cls(
            input_source=Path(job_args.input_source),
            output_directory=Path(job_args.output_directory),
            user_settings=user_settings_from_args,
        )


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    etl = BergamoEtl.from_args(sys_args)
    etl.run_job()
