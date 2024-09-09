"""Module to map bergamo metadata into a session model."""

import argparse
import bisect
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Dict, List, Tuple, Union
from zoneinfo import ZoneInfo

import numpy as np
from aind_data_schema.components.coordinates import (
    Axis,
    AxisName,
    RelativePosition,
    Rotation3dTransform,
    Translation3dTransform,
)
from aind_data_schema.components.devices import Software
from aind_data_schema.components.stimulus import (
    PhotoStimulation,
    PhotoStimulationGroup,
)
from aind_data_schema.core.session import (
    DetectorConfig,
    FieldOfView,
    LaserConfig,
    Modality,
    RewardDeliveryConfig,
    RewardSolution,
    RewardSpoutConfig,
    Session,
    SpoutSide,
    Stack,
    StackChannel,
    StimulusEpoch,
    StimulusModality,
    Stream,
    TriggerType,
)
from aind_data_schema_models.units import PowerUnit
from ScanImageTiffReader import ScanImageTiffReader

from aind_metadata_mapper.bergamo.models import JobSettings
from aind_metadata_mapper.core import GenericEtl, JobResponse


# This class makes it easier to flag which tif files are which expected type
class TifFileGroup(str, Enum):
    """Type of stimulation a group of files belongs to"""

    BEHAVIOR = "behavior"
    PHOTOSTIM = "photostim"
    SPONTANEOUS = "spontaneous"
    STACK = "stack"


# This class will hold the metadata information pulled from the tif files
# with minimal parsing.
@dataclass(frozen=True)
class RawImageInfo:
    """Raw metadata from a tif file"""

    reader_metadata_header: dict
    reader_metadata_json: dict
    # The reader descriptions for the last tif file
    reader_descriptions: List[dict]
    # Looks like [620, 800, 800]
    # [num_of_frames, pixel_width, pixel_height]?
    reader_shape: List[int]


class BergamoEtl(GenericEtl[JobSettings]):
    """Class to manage transforming bergamo data files into a Session object"""

    def __init__(
        self,
        job_settings: Union[JobSettings, str],
    ):
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

    def get_tif_file_locations(self) -> Dict[str, List[Path]]:
        """Scans the input source directory and returns a dictionary of file
        groups in an ordered list. For example, if the directory had
        [neuron2_00001.tif, neuron2_00002.tif, stackPost_00001.tif,
        stackPost_00002.tif, stackPost_00003.tif], then it will return
        { "neuron2": [neuron2_00001.tif, neuron2_00002.tif],
         "stackPost":
           [stackPost_00001.tif, stackPost_00002.tif, stackPost_00003.tif]
        }
        """
        compiled_regex = re.compile(r"^(.*)_.*?(\d+).tif+$")
        tif_file_map = {}
        for root, dirs, files in os.walk(self.job_settings.input_source):
            for name in files:
                matched = re.match(compiled_regex, name)
                if matched:
                    groups = matched.groups()
                    file_stem = groups[0]
                    # tif_number = groups[1]
                    tif_filepath = Path(os.path.join(root, name))
                    if tif_file_map.get(file_stem) is None:
                        tif_file_map[file_stem] = [tif_filepath]
                    else:
                        bisect.insort(tif_file_map[file_stem], tif_filepath)

            # Only scan the top level files
            break
        return tif_file_map

    @staticmethod
    def flat_dict_to_nested(flat: dict, key_delim: str = ".") -> dict:
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

    # This methods parses a single file into RawImageInfo dataclass
    def extract_raw_info_from_file(self, file_path: Path) -> RawImageInfo:
        """
        Use ScanImageTiffReader to read medata from a single file and parse
        it into a RawImageInfo object
        Parameters
        ----------
        file_path : Path

        Returns
        -------
        RawImageInfo

        """
        with ScanImageTiffReader(str(file_path)) as reader:
            reader_metadata = reader.metadata()
            reader_shape = reader.shape()
            reader_descriptions = [
                dict(
                    [
                        (s.split(" = ", 1)[0], s.split(" = ", 1)[1])
                        for s in reader.description(i).strip().split("\n")
                    ]
                )
                for i in range(0, len(reader))
            ]

        metadata_first_part = reader_metadata.split("\n\n")[0]
        flat_metadata_header_dict = dict(
            [
                (s.split(" = ", 1)[0], s.split(" = ", 1)[1])
                for s in metadata_first_part.split("\n")
            ]
        )
        metadata_dict = self.flat_dict_to_nested(flat_metadata_header_dict)
        reader_metadata_json = json.loads(reader_metadata.split("\n\n")[1])
        # Move SI dictionary up one level
        if "SI" in metadata_dict.keys():
            si_contents = metadata_dict.pop("SI")
            metadata_dict.update(si_contents)
        return RawImageInfo(
            reader_shape=reader_shape,
            reader_metadata_header=metadata_dict,
            reader_metadata_json=reader_metadata_json,
            reader_descriptions=reader_descriptions,
        )

    @staticmethod
    def map_raw_image_info_to_tif_file_group(
        raw_image_info: RawImageInfo,
    ) -> TifFileGroup:
        """
        Map raw image info to a tiff file group type
        Parameters
        ----------
        raw_image_info : RawImageInfo

        Returns
        -------
        TifFileGroup

        """
        header = raw_image_info.reader_metadata_header
        if header.get("hPhotostim", {}).get("status") in [
            "'Running'",
            "Running",
        ]:
            return TifFileGroup.PHOTOSTIM
        elif (
            header.get("hIntegrationRoiManager", {}).get("enable") == "true"
            and header.get("hIntegrationRoiManager", {}).get(
                "outputChannelsEnabled"
            )
            == "true"
            and header.get("extTrigEnable", {}) == "1"
        ):
            return TifFileGroup.BEHAVIOR
        elif header.get("hStackManager", {}).get("enable") == "true":
            return TifFileGroup.STACK
        else:
            return TifFileGroup.SPONTANEOUS

    def extract_parsed_metadata_info_from_files(
        self, tif_file_locations: Dict[str, List[Path]]
    ) -> Dict[
        Tuple[str, TifFileGroup], List[Union[RawImageInfo, List[List[Path]]]]
    ]:
        """
        Loop through list of files and use ScanImageTiffReader to read metadata
        Parameters
        ----------
        tif_file_locations : Dict[str, List[Path]]

        Returns
        -------
        Dict[Tuple[str, TifFileGroup],
        List[Union[RawImageInfo, List[List[Path]]]]]

        """
        parsed_map = {}
        for file_stem, files in tif_file_locations.items():
            # number_of_files = len(files)
            last_idx = -1
            metadata_extracted = False
            while not metadata_extracted:
                try:
                    last_file = files[last_idx]
                    raw_info = self.extract_raw_info_from_file(last_file)
                    metadata_extracted = True
                except Exception as e:
                    logging.warning(e)
                    last_idx -= 1
            raw_info_first_file = self.extract_raw_info_from_file(files[0])
            # parsed_info = parse_raw_metadata(
            #     raw_image_info=raw_info, number_of_files=number_of_files
            # )
            tif_file_group = self.map_raw_image_info_to_tif_file_group(
                raw_image_info=raw_info
            )
            parsed_map[(file_stem, tif_file_group)] = [
                raw_info,
                [files],
                raw_info_first_file,
            ]
        return parsed_map

    @staticmethod
    def _create_detector_config(detector_name: str) -> DetectorConfig:
        """Creates Detector Config"""
        detector = DetectorConfig.model_construct(
            name=detector_name,
            exposure_time=None,
            trigger_type=TriggerType.INTERNAL.value,
        )
        return detector

    # TODO: Make this way less complex
    def run_job(self) -> JobResponse:  # noqa: C901
        """Run the etl job and return a JobResponse."""
        tif_file_locations = self.get_tif_file_locations()
        # parse metadata
        parsed_metadata = self.extract_parsed_metadata_info_from_files(
            tif_file_locations=tif_file_locations
        )
        stack_file_info = [
            (k, v)
            for k, v in parsed_metadata.items()
            if k[1] == TifFileGroup.STACK
        ]
        spont_file_info = [
            (k, v)
            for k, v in parsed_metadata.items()
            if k[1] == TifFileGroup.SPONTANEOUS
        ]
        behavior_file_info = [
            (k, v)
            for k, v in parsed_metadata.items()
            if k[1] == TifFileGroup.BEHAVIOR
        ]
        photo_stim_file_info = [
            (k, v)
            for k, v in parsed_metadata.items()
            if k[1] == TifFileGroup.PHOTOSTIM
        ]
        first_tiff_metadata_header = parsed_metadata[
            list(parsed_metadata.keys())[0]
        ][0].reader_metadata_header
        # FROM RIG JSON: filter_names, detector_name, daq_name
        channel_dict = {
            1: {
                "channel_name": "Ch1",
                "light_source_name": self.job_settings.imaging_laser_name,
                "filter_names": self.job_settings.ch1_filter_names,
                "detector_name": self.job_settings.ch1_detector_name,
                "excitation_wavelength": (
                    self.job_settings.imaging_laser_wavelength
                ),
                "daq_name": self.job_settings.ch1_daq_name,
            },
            2: {
                "channel_name": "Ch2",
                "light_source_name": self.job_settings.imaging_laser_name,
                "filter_names": self.job_settings.ch2_filter_names,
                "detector_name": self.job_settings.ch2_detector_name,
                "excitation_wavelength": (
                    self.job_settings.imaging_laser_wavelength
                ),
                "daq_name": self.job_settings.ch2_daq_name,
            },
        }
        laser_dict = {
            "imaging_laser": {"power_index": 0},
            "photostim_laser": {"power_index": 1},
        }
        FOV_1x_micron = 1000
        lickportposition = RelativePosition(
            device_position_transformations=[
                Translation3dTransform(
                    translation=self.job_settings.starting_lickport_position
                ),
                # this is the standard position for BCI task
                Rotation3dTransform(rotation=[0] * 9),
            ],
            # this is the standard position for BCI task
            device_origin="tip of the lickspout",
            device_axes=[
                Axis(name=AxisName.X, direction="lateral motion"),
                Axis(
                    name=AxisName.Y,
                    direction=(
                        "rostro-caudal motion positive is towards mouse,"
                        " negative is away"
                    ),
                ),
                Axis(name=AxisName.Z, direction="up/down"),
            ],
        )

        reward_spout_config = RewardSpoutConfig(
            side=SpoutSide.CENTER,
            starting_position=lickportposition,
            variable_position=True,
        )
        reward_delivery = RewardDeliveryConfig(
            reward_solution=RewardSolution.WATER,
            reward_spouts=[reward_spout_config],
        )
        behavior_software = Software(
            name="pyBpod",
            version="1.8.2",  # hard coded
            url="https://github.com/pybpod/pybpod",
        )
        pybpod_script = Software(
            name="pybpod_basic.py",  # file name
            version="2d77d15",  # commit#
            url=(
                "https://github.com/rozmar/BCI-motor-control/blob/main/"
                "BCI-pybpod-protocols/bci_basic.py"
            ),
            parameters={},
        )  # can I do this?
        photostim_software = Software(
            name="ScanImage",
            version="{}.{}.{}".format(
                first_tiff_metadata_header["VERSION_MAJOR"],
                first_tiff_metadata_header["VERSION_MINOR"],
                first_tiff_metadata_header["VERSION_UPDATE"],
            ),  # hard coded
            url="https://www.mbfbioscience.com/products/scanimage/",
        )  # hard coded
        # detector1 = DetectorConfig.model_construct(
        #     name=self.job_settings.ch1_detector_name,
        #     exposure_time=None,
        #     trigger_type=TriggerType.INTERNAL.value,
        # )
        # detector2 = DetectorConfig.model_construct(
        #     name=self.job_settings.ch2_detector_name,
        #     exposure_time=None,
        #     trigger_type=TriggerType.INTERNAL.value,
        # )
        # ch_detectors = [detector1, detector2]
        all_stream_start_times = []
        all_stream_end_times = []
        streams = []
        stim_epochs = []
        # ONLY 2P STREAM DURING STACKS
        for stack_file_info_now in stack_file_info:
            # generate tiff list
            tiff_stem = stack_file_info_now[0][0]
            tiff_list = []
            for pathnow in stack_file_info_now[1][1][0]:
                tiff_list.append(Path(pathnow).name)
            tiff_header = stack_file_info_now[1][0].reader_metadata_header
            last_frame_description = stack_file_info_now[1][
                0
            ].reader_descriptions[-1]
            # THIS THING REPEATS FOR EVERY STREAM
            z_list = np.asarray(
                tiff_header["hStackManager"]["zs"].strip("[]").split(" "),
                float,
            )
            z_start = (
                np.min(z_list)
                - np.median(z_list)
                + self.job_settings.fov_imaging_depth
            )
            z_end = (
                np.max(z_list)
                - np.median(z_list)
                + self.job_settings.fov_imaging_depth
            )
            z_step = float(tiff_header["hStackManager"]["stackZStepSize"])
            channel_nums = np.asarray(
                tiff_header["hChannels"]["channelSave"].strip("[]").split(" "),
                int,
            )
            daq_names = []
            detectors = []
            for channel_num in channel_nums:
                daq_names.append(channel_dict[channel_num]["daq_name"])
                detectors.append(
                    self._create_detector_config(
                        channel_dict[channel_num]["detector_name"]
                    )
                )

            channels = []
            start_time_corrected = (
                last_frame_description["epoch"]
                .strip("[]")
                .replace("  ", " 0")
                .split(" ")
            )
            start_time_corrected = " ".join(
                start_time_corrected[:-1]
                + [
                    str(int(np.floor(float(start_time_corrected[-1])))).zfill(
                        2
                    ),
                    str(
                        int(1000000 * (float(start_time_corrected[-1]) % 1))
                    ).zfill(6),
                ]
            )
            stream_start_time = datetime.strptime(
                start_time_corrected, "%Y %m %d %H %M %S %f"
            ).replace(tzinfo=ZoneInfo(self.job_settings.timezone))
            stream_start_time = stream_start_time.replace(
                tzinfo=ZoneInfo(self.job_settings.timezone)
            )
            stream_end_time = stream_start_time + timedelta(
                seconds=float(last_frame_description["frameTimestamps_sec"])
            )
            # THIS THING REPEATS FOR EVERY STREAM
            all_stream_start_times.append(stream_start_time)
            all_stream_end_times.append(stream_end_time)
            for channel_num in channel_nums:
                channels.append(
                    StackChannel(
                        start_depth=z_start,
                        end_depth=z_end,
                        channel_name=channel_dict[channel_num]["channel_name"],
                        light_source_name=channel_dict[channel_num][
                            "light_source_name"
                        ],
                        filter_names=channel_dict[channel_num]["filter_names"],
                        detector_name=channel_dict[channel_num][
                            "detector_name"
                        ],
                        excitation_wavelength=channel_dict[channel_num][
                            "excitation_wavelength"
                        ],
                        excitation_power=np.asarray(
                            tiff_header["hBeams"]["powers"]
                            .strip("[]")
                            .split(" "),
                            float,
                        )[laser_dict["imaging_laser"]["power_index"]],
                        # from tiff header,
                        excitation_power_unit=PowerUnit.PERCENT,
                        filter_wheel_index=0,
                    )
                )
            zstack = Stack(
                channels=channels,
                number_of_planes=int(
                    tiff_header["hStackManager"]["numSlices"]
                ),
                step_size=z_step,
                number_of_plane_repeats_per_volume=int(
                    tiff_header["hStackManager"]["framesPerSlice"]
                ),
                number_of_volume_repeats=int(
                    tiff_header["hStackManager"]["numVolumes"]
                ),
                fov_coordinate_ml=self.job_settings.fov_coordinate_ml,
                fov_coordinate_ap=self.job_settings.fov_coordinate_ap,
                fov_reference="there is no reference",
                fov_width=int(tiff_header["hRoiManager"]["pixelsPerLine"]),
                fov_height=int(tiff_header["hRoiManager"]["linesPerFrame"]),
                magnification=str(
                    tiff_header["hRoiManager"]["scanZoomFactor"]
                ),
                fov_scale_factor=(
                    FOV_1x_micron
                    / float(tiff_header["hRoiManager"]["scanZoomFactor"])
                )
                / float(tiff_header["hRoiManager"]["linesPerFrame"]),
                # microns per pixel
                frame_rate=float(tiff_header["hRoiManager"]["scanFrameRate"]),
                targeted_structure=self.job_settings.fov_targeted_structure,
            )
            stream_stack = Stream(
                stream_start_time=stream_start_time,
                stream_end_time=stream_end_time,
                daq_names=daq_names,
                light_sources=[
                    LaserConfig(
                        name=self.job_settings.imaging_laser_name,
                        # from rig json
                        wavelength=self.job_settings.imaging_laser_wavelength,
                        # user set value
                        excitation_power=np.asarray(
                            tiff_header["hBeams"]["powers"]
                            .strip("[]")
                            .split(" "),
                            float,
                        )[laser_dict["imaging_laser"]["power_index"]],
                        excitation_power_unit=PowerUnit.PERCENT,
                    )
                ],
                stack_parameters=zstack,
                stream_modalities=[Modality.POPHYS],
                detectors=detectors,
                notes="tiff_stem:{}".format(tiff_stem),
            )
            streams.append(stream_stack)

        # ONLY 2P STREAM DURING SPONT
        for spont_file_info_now in spont_file_info:
            # generate tiff list
            tiff_stem = spont_file_info_now[0][0]
            tiff_list = []
            for pathnow in spont_file_info_now[1][1][0]:
                tiff_list.append(Path(pathnow).name)
            tiff_header = spont_file_info_now[1][0].reader_metadata_header
            last_frame_description = spont_file_info_now[1][
                0
            ].reader_descriptions[-1]
            # THIS THING REPEATS FOR EVERY STREAM
            z_list = np.asarray(
                tiff_header["hStackManager"]["zs"].strip("[]").split(" "),
                float,
            )
            # z_start = (
            #     np.min(z_list)
            #     - np.median(z_list)
            #     + self.job_settings.fov_imaging_depth
            # )
            # z_end = (
            #     np.max(z_list)
            #     - np.median(z_list)
            #     + self.job_settings.fov_imaging_depth
            # )
            # z_step = float(tiff_header["hStackManager"]["stackZStepSize"])
            channel_nums = np.asarray(
                tiff_header["hChannels"]["channelSave"].strip("[]").split(" "),
                int,
            )
            daq_names = []
            detectors = []
            for channel_num in channel_nums:
                daq_names.append(channel_dict[channel_num]["daq_name"])
                detectors.append(
                    self._create_detector_config(
                        channel_dict[channel_num]["detector_name"]
                    )
                )

            # channels = []
            start_time_corrected = (
                last_frame_description["epoch"]
                .strip("[]")
                .replace("  ", " 0")
                .split(" ")
            )
            start_time_corrected = " ".join(
                start_time_corrected[:-1]
                + [
                    str(int(np.floor(float(start_time_corrected[-1])))).zfill(
                        2
                    ),
                    str(
                        int(1000000 * (float(start_time_corrected[-1]) % 1))
                    ).zfill(6),
                ]
            )
            stream_start_time = datetime.strptime(
                start_time_corrected, "%Y %m %d %H %M %S %f"
            )
            stream_start_time = stream_start_time.replace(
                tzinfo=ZoneInfo(self.job_settings.timezone)
            )
            stream_end_time = stream_start_time + timedelta(
                seconds=float(last_frame_description["frameTimestamps_sec"])
            )
            # THIS THING REPEATS FOR EVERY STREAM
            all_stream_start_times.append(stream_start_time)
            all_stream_end_times.append(stream_end_time)
            fov_2p = FieldOfView(
                index=0,
                # multi-plane will have multiple - in a list
                imaging_depth=self.job_settings.fov_imaging_depth,
                # in microns
                fov_coordinate_ml=self.job_settings.fov_coordinate_ml,
                fov_coordinate_ap=self.job_settings.fov_coordinate_ap,
                fov_reference="there is no reference",
                fov_width=int(tiff_header["hRoiManager"]["pixelsPerLine"]),
                fov_height=int(tiff_header["hRoiManager"]["linesPerFrame"]),
                magnification=str(
                    tiff_header["hRoiManager"]["scanZoomFactor"]
                ),
                fov_scale_factor=(
                    FOV_1x_micron
                    / float(tiff_header["hRoiManager"]["scanZoomFactor"])
                )
                / float(tiff_header["hRoiManager"]["linesPerFrame"]),
                # microns per pixel
                frame_rate=float(tiff_header["hRoiManager"]["scanFrameRate"]),
                targeted_structure=self.job_settings.fov_targeted_structure,
            )
            stream_2p = Stream(
                stream_start_time=stream_start_time,
                # calculate - specify timezone # each basename is a separate
                # stream
                stream_end_time=stream_end_time,  # calculate
                daq_names=daq_names,  # from the rig json
                light_sources=[
                    LaserConfig(
                        name=self.job_settings.imaging_laser_name,
                        # from rig json
                        wavelength=self.job_settings.imaging_laser_wavelength,
                        # user set value
                        excitation_power=np.asarray(
                            tiff_header["hBeams"]["powers"]
                            .strip("[]")
                            .split(" "),
                            float,
                        )[laser_dict["imaging_laser"]["power_index"]],
                        # from tiff header,
                        excitation_power_unit=PowerUnit.PERCENT,
                    )
                ],
                ophys_fovs=[fov_2p],
                # multiple planes come here
                stream_modalities=[Modality.POPHYS],
                detectors=detectors,
                notes="tiff_stem:{}".format(tiff_stem),
            )
            streams.append(stream_2p)

            stim_epoch_spont = StimulusEpoch(
                stimulus_start_time=stream_start_time,
                # datetime#basenames are separate
                stimulus_end_time=stream_end_time,  # datetime,
                stimulus_name="spontaneous activity",  # user defined in script
                stimulus_modalities=[StimulusModality.NONE],
                notes="absence of any kind of stimulus",
                output_parameters={
                    "tiff_files": tiff_list,
                    "tiff_stem": tiff_stem,
                },
            )
            stim_epochs.append(stim_epoch_spont)

        # 2P + behavior + behavior video STREAM DURING BEHAVIOR
        for behavior_file_info_now in behavior_file_info:
            # generate tiff list
            tiff_stem = behavior_file_info_now[0][0]
            tiff_list = []
            for pathnow in behavior_file_info_now[1][1][0]:
                tiff_list.append(Path(pathnow).name)

            tiff_header = behavior_file_info_now[1][0].reader_metadata_header
            last_frame_description = behavior_file_info_now[1][
                0
            ].reader_descriptions[-1]
            # THIS THING REPEATS FOR EVERY STREAM

            # z_list = np.asarray(
            #     tiff_header["hStackManager"]["zs"].strip("[]").split(" "),
            #     float,
            # )
            # z_start = (
            #     np.min(z_list)
            #     - np.median(z_list)
            #     + self.job_settings.fov_imaging_depth
            # )
            # z_end = (
            #     np.max(z_list)
            #     - np.median(z_list)
            #     + self.job_settings.fov_imaging_depth
            # )
            # z_step = float(tiff_header["hStackManager"]["stackZStepSize"])
            channel_nums = np.asarray(
                tiff_header["hChannels"]["channelSave"].strip("[]").split(" "),
                int,
            )
            daq_names = []
            detectors = []
            for channel_num in channel_nums:
                daq_names.append(channel_dict[channel_num]["daq_name"])
                detectors.append(
                    self._create_detector_config(
                        channel_dict[channel_num]["detector_name"]
                    )
                )
            # channels = []
            start_time_corrected = (
                last_frame_description["epoch"]
                .strip("[]")
                .replace("  ", " 0")
                .split(" ")
            )
            start_time_corrected = " ".join(
                start_time_corrected[:-1]
                + [
                    str(int(np.floor(float(start_time_corrected[-1])))).zfill(
                        2
                    ),
                    str(
                        int(1000000 * (float(start_time_corrected[-1]) % 1))
                    ).zfill(6),
                ]
            )
            stream_start_time = datetime.strptime(
                start_time_corrected, "%Y %m %d %H %M %S %f"
            )
            stream_start_time = stream_start_time.replace(
                tzinfo=ZoneInfo(self.job_settings.timezone)
            )
            stream_end_time = stream_start_time + timedelta(
                seconds=float(last_frame_description["frameTimestamps_sec"])
            )
            # THIS THING REPEATS FOR EVERY STREAM
            all_stream_start_times.append(stream_start_time)
            all_stream_end_times.append(stream_end_time)
            fov_2p = FieldOfView(
                index=0,
                # multi-plane will have multiple - in a list
                imaging_depth=self.job_settings.fov_imaging_depth,
                # in microns
                fov_coordinate_ml=self.job_settings.fov_coordinate_ml,
                fov_coordinate_ap=self.job_settings.fov_coordinate_ap,
                fov_reference="there is no reference",
                fov_width=int(tiff_header["hRoiManager"]["pixelsPerLine"]),
                fov_height=int(tiff_header["hRoiManager"]["linesPerFrame"]),
                magnification=str(
                    tiff_header["hRoiManager"]["scanZoomFactor"]
                ),
                fov_scale_factor=(
                    FOV_1x_micron
                    / float(tiff_header["hRoiManager"]["scanZoomFactor"])
                )
                / float(tiff_header["hRoiManager"]["linesPerFrame"]),
                # microns per pixel
                frame_rate=float(tiff_header["hRoiManager"]["scanFrameRate"]),
                targeted_structure=self.job_settings.fov_targeted_structure,
            )

            stream_modalities = [Modality.POPHYS, Modality.BEHAVIOR]
            if len(self.job_settings.behavior_camera_names) > 0:
                camera_names = self.job_settings.behavior_camera_names
                stream_modalities.append(Modality.BEHAVIOR_VIDEOS)
            else:
                camera_names = []

            stream_2p = Stream(
                stream_start_time=stream_start_time,
                # calculate - specify timezone # each basename is a separate
                # stream
                stream_end_time=stream_end_time,  # calculate
                daq_names=daq_names,  # from the rig json
                light_sources=[
                    LaserConfig(
                        name=self.job_settings.imaging_laser_name,
                        # from rig json
                        wavelength=self.job_settings.imaging_laser_wavelength,
                        # user set value
                        excitation_power=np.asarray(
                            tiff_header["hBeams"]["powers"]
                            .strip("[]")
                            .split(" "),
                            float,
                        )[laser_dict["imaging_laser"]["power_index"]],
                        # from tiff header,
                        excitation_power_unit=PowerUnit.PERCENT,
                    )
                ],
                ophys_fovs=[fov_2p],
                # multiple planes come here
                stream_modalities=stream_modalities,
                camera_names=camera_names,
                detectors=detectors,
                notes="tiff_stem:{}".format(tiff_stem),
            )
            streams.append(stream_2p)

            hit_rate_trials_0_10 = self.job_settings.hit_rate_trials_0_10
            hit_rate_trials_20_40 = self.job_settings.hit_rate_trials_20_40

            stim_epoch_behavior = StimulusEpoch(
                stimulus_start_time=stream_start_time,
                # datetime#basenames are separate
                stimulus_end_time=stream_end_time,  # datetime,
                stimulus_name=self.job_settings.behavior_task_name,
                # user defined in script
                software=[behavior_software],
                script=pybpod_script,
                stimulus_modalities=[StimulusModality.AUDITORY],
                # ,StimulusModality.TACTILE],# tactile not in this version yet
                stimulus_parameters=[],
                # opticalBCI class to be added in future
                stimulus_device_names=self.job_settings.stimulus_device_names,
                # from json file, to be added (speaker, bpod ID, )
                output_parameters={
                    "tiff_files": tiff_list,
                    "tiff_stem": tiff_stem,
                    "hit_rate_trials_0_10": hit_rate_trials_0_10,
                    "hit_rate_trials_20_40": hit_rate_trials_20_40,
                    "total_hits": self.job_settings.total_hits,
                    "average_hit_rate": self.job_settings.average_hit_rate,
                },  # hit rate, time to reward, ...?
                trials_total=self.job_settings.trial_num,
                # trials_rewarded = ,  # not using BPOD info yet
            )
            stim_epochs.append(stim_epoch_behavior)

        # 2P + behavior + behavior video STREAM DURING BEHAVIOR
        for photo_stim_file_info_now in photo_stim_file_info:
            # generate tiff list
            tiff_stem = photo_stim_file_info_now[0][0]
            tiff_list = []
            for pathnow in photo_stim_file_info_now[1][1][0]:
                tiff_list.append(Path(pathnow).name)
            tiff_header = photo_stim_file_info_now[1][0].reader_metadata_header
            last_frame_description = photo_stim_file_info_now[1][
                0
            ].reader_descriptions[-1]

            # THIS THING REPEATS FOR EVERY STREAM

            # z_list = np.asarray(
            #     tiff_header["hStackManager"]["zs"].strip("[]").split(" "),
            #     float,
            # )
            # z_start = (
            #     np.min(z_list)
            #     - np.median(z_list)
            #     + self.job_settings.fov_imaging_depth
            # )
            # z_end = (
            #     np.max(z_list)
            #     - np.median(z_list)
            #     + self.job_settings.fov_imaging_depth
            # )
            # z_step = float(tiff_header["hStackManager"]["stackZStepSize"])
            channel_nums = np.asarray(
                tiff_header["hChannels"]["channelSave"].strip("[]").split(" "),
                int,
            )
            daq_names = []
            detectors = []
            for channel_num in channel_nums:
                daq_names.append(channel_dict[channel_num]["daq_name"])
                detectors.append(
                    self._create_detector_config(
                        channel_dict[channel_num]["detector_name"]
                    )
                )
            # channels = []
            start_time_corrected = (
                last_frame_description["epoch"]
                .strip("[]")
                .replace("  ", " 0")
                .split(" ")
            )
            start_time_corrected = " ".join(
                start_time_corrected[:-1]
                + [
                    str(int(np.floor(float(start_time_corrected[-1])))).zfill(
                        2
                    ),
                    str(
                        int(1000000 * (float(start_time_corrected[-1]) % 1))
                    ).zfill(6),
                ]
            )
            stream_start_time = datetime.strptime(
                start_time_corrected, "%Y %m %d %H %M %S %f"
            )
            stream_start_time = stream_start_time.replace(
                tzinfo=ZoneInfo(self.job_settings.timezone)
            )
            stream_end_time = stream_start_time + timedelta(
                seconds=float(last_frame_description["frameTimestamps_sec"])
            )
            # THIS THING REPEATS FOR EVERY STREAM
            all_stream_start_times.append(stream_start_time)
            all_stream_end_times.append(stream_end_time)
            fov_2p = FieldOfView(
                index=0,
                # multi-plane will have multiple - in a list
                imaging_depth=self.job_settings.fov_imaging_depth,
                # in microns
                fov_coordinate_ml=self.job_settings.fov_coordinate_ml,
                fov_coordinate_ap=self.job_settings.fov_coordinate_ap,
                fov_reference="there is no reference",
                fov_width=int(tiff_header["hRoiManager"]["pixelsPerLine"]),
                fov_height=int(tiff_header["hRoiManager"]["linesPerFrame"]),
                magnification=str(
                    tiff_header["hRoiManager"]["scanZoomFactor"]
                ),
                fov_scale_factor=(
                    FOV_1x_micron
                    / float(tiff_header["hRoiManager"]["scanZoomFactor"])
                )
                / float(tiff_header["hRoiManager"]["linesPerFrame"]),
                # microns per pixel
                frame_rate=float(tiff_header["hRoiManager"]["scanFrameRate"]),
                targeted_structure=self.job_settings.fov_targeted_structure,
            )
            stream_2p = Stream(
                stream_start_time=stream_start_time,
                # calculate - specify timezone # each basename is a separate
                # stream
                stream_end_time=stream_end_time,  # calculate
                daq_names=daq_names,  # from the rig json
                light_sources=[
                    LaserConfig(
                        name=self.job_settings.imaging_laser_name,
                        # from rig json
                        wavelength=self.job_settings.imaging_laser_wavelength,
                        # user set value
                        excitation_power=np.asarray(
                            tiff_header["hBeams"]["powers"]
                            .strip("[]")
                            .split(" "),
                            float,
                        )[laser_dict["imaging_laser"]["power_index"]],
                        # from tiff header,
                        excitation_power_unit=PowerUnit.PERCENT,
                    )
                ],
                ophys_fovs=[fov_2p],
                # multiple planes come here
                stream_modalities=[Modality.POPHYS],
                detectors=detectors,
                notes="tiff_stem:{}".format(tiff_stem),
            )
            streams.append(stream_2p)

            ####

            photostim_groups = []
            group_order = (
                np.asarray(
                    tiff_header["hPhotostim"]["sequenceSelectedStimuli"]
                    .strip("[]")
                    .split(" ")
                    * 100,
                    int,
                )
                - 1
            )
            num_total_repetitions = len(photo_stim_file_info_now[1][1][0])
            group_order = group_order[:num_total_repetitions]
            group_powers = []
            for photostim_group_i, photostim_group in enumerate(
                photo_stim_file_info_now[1][0].reader_metadata_json[
                    "RoiGroups"
                ]["photostimRoiGroups"]
            ):
                number_of_neurons = int(
                    np.array(
                        photostim_group["rois"][1]["scanfields"]["slmPattern"]
                    ).shape[0]
                )
                stimulation_laser_power = Decimal(
                    str(photostim_group["rois"][1]["scanfields"]["powers"])
                )
                number_spirals = int(
                    photostim_group["rois"][1]["scanfields"]["repetitions"]
                )
                spiral_duration = Decimal(
                    str(photostim_group["rois"][1]["scanfields"]["duration"])
                )
                inter_spiral_interval = Decimal(
                    str(
                        photostim_group["rois"][2]["scanfields"]["duration"]
                        + photostim_group["rois"][0]["scanfields"]["duration"]
                    )
                )

                number_of_trials = sum(group_order == photostim_group_i)
                photostim_groups.append(
                    PhotoStimulationGroup(
                        group_index=photostim_group_i + 1,
                        number_of_neurons=number_of_neurons,
                        stimulation_laser_power=stimulation_laser_power,
                        stimulation_laser_power_unit=PowerUnit.PERCENT,
                        number_trials=number_of_trials,
                        number_spirals=number_spirals,
                        spiral_duration=spiral_duration,
                        inter_spiral_interval=inter_spiral_interval,
                    )
                )
                group_powers.append(stimulation_laser_power)

            photostim = PhotoStimulation(
                stimulus_name="2p photostimulation",
                number_groups=len(photostim_groups),
                # tiff header
                groups=photostim_groups,
                inter_trial_interval=Decimal(
                    float(
                        photo_stim_file_info_now[1][2].reader_descriptions[-1][
                            "nextFileMarkerTimestamps_sec"
                        ]
                    )
                ),
            )  # from Jon's script - seconds
            wavelength = self.job_settings.photostim_laser_wavelength
            stim_epoch_photostim = StimulusEpoch(
                stimulus_start_time=stream_start_time,
                stimulus_end_time=stream_end_time,  # datetime,
                stimulus_name="2p photostimulation",  # user defined in script
                software=[photostim_software],
                stimulus_modalities=[StimulusModality.OPTOGENETICS],
                stimulus_parameters=[photostim],
                # opticalBCI class to be added in future
                stimulus_device_names=self.job_settings.stimulus_device_names,
                light_source_config=[
                    LaserConfig(
                        # from rig json
                        name=self.job_settings.photostim_laser_name,
                        wavelength=wavelength,
                        # user set value
                        excitation_power=np.nanmean(group_powers),
                        # from tiff header,
                        excitation_power_unit=PowerUnit.PERCENT,
                    )
                ],
                output_parameters={
                    "tiff_files": tiff_list,
                    "tiff_stem": tiff_stem,
                },
            )
            stim_epochs.append(stim_epoch_photostim)
        # TODO: remove model_construct, fill in exposure time from acq machine
        s = Session(
            experimenter_full_name=self.job_settings.experimenter_full_name,
            # user added
            session_start_time=min(all_stream_start_times),
            session_end_time=max(all_stream_end_times),
            session_type=self.job_settings.session_type,
            iacuc_protocol=self.job_settings.iacuc_protocol,
            rig_id=self.job_settings.rig_id,  # from rig json
            # calibrations = [Calibration(calibration_date = ,
            #                        device_name = '',#from rig json)
            #                        description = 'laser calibration',
            #                        input ={'power_percent':[]},
            #                        output = {'power_mW':[]})],
            subject_id=self.job_settings.subject_id,  # user added
            reward_delivery=reward_delivery,
            data_streams=streams,
            mouse_platform_name=self.job_settings.mouse_platform_name,
            # from rig json
            active_mouse_platform=self.job_settings.active_mouse_platform,
            stimulus_epochs=stim_epochs,
            notes=self.job_settings.notes,  # user added
        )
        job_response = self._load(s, self.job_settings.output_directory)

        return job_response

    # TODO: The following can probably be abstracted
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
            "-j",
            "--job-settings",
            required=True,
            type=str,
            help=(
                r"""
                Custom settings defined by the user defined as a json
                 string. For example: -j
                 '{
                 "input_source":"/directory/to/read/from",
                 "output_directory":"/directory/to/write/to",
                 "experimenter_full_name":["John Smith","Jane Smith"],
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
        job_settings_from_args = JobSettings.model_validate_json(
            job_args.job_settings
        )
        return cls(
            job_settings=job_settings_from_args,
        )


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    etl = BergamoEtl.from_args(sys_args)
    etl.run_job()
