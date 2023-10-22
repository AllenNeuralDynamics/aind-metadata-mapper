"""Module to map bergamo metadata into a session model"""

import json
import logging
import os
from pathlib import Path
from typing import Any
import numpy as np

from aind_data_schema.session import Session, Stream, Modality, TriggerType, Laser, Detector, FieldOfView
from aind_data_schema.utils.units import SizeUnit, PowerUnit
from aind_data_schema.stimulus import (
    PhotoStimulation,
    PhotoStimulationGroup,
    StimulusEpoch,
)
from ScanImageTiffReader import ScanImageTiffReader
from typing import Any, List

from aind_metadata_mapper.core import BaseEtl
from dataclasses import dataclass
from datetime import datetime, time


@dataclass(frozen=True)
class RawImageInfo:
    metadata: str
    description0: str
    shape: List[int]


@dataclass(frozen=True)
class ParsedMetadata:
    metadata: dict
    roi_data: dict
    roi_metadata: dict
    frame_rate: str
    num_planes: int
    shape: List[int]
    description_first_frame: dict
    movie_start_time: datetime


class BergamoEtl(BaseEtl):
    @staticmethod
    def _flat_dict_to_nested(flat: dict, key_delim="."):
        # https://stackoverflow.com/a/50607551

        def __nest_dict_rec(k, v, out) -> None:
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
        """Parses metadata from tiff_reader."""

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
        try:
            z_collection = metadata["hFastZ"]["userZs"]
            num_planes = len(z_collection)
        except Exception as e:  # new scanimage version
            logging.error(
                f"Multiple planes not handled in metadata collection. "
                f"HANDLE ME!!!: {repr(e)}"
            )
            if metadata["hFastZ"]["enable"] == "true":
                num_planes = 1
            else:
                num_planes = 1

        roi_metadata = metadata["json"]["RoiGroups"]["imagingRoiGroup"]["rois"]

        if type(roi_metadata) == dict:
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

        data = {}
        data["fs"] = frame_rate
        data["nplanes"] = num_planes
        data["nrois"] = num_rois  # or irow.shape[1]?
        if data["nrois"] == 1:
            data["mesoscan"] = 0
        else:
            data["mesoscan"] = 1

        if data["mesoscan"]:
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
    def _get_si_file_from_dir(source_dir: Path) -> Path:
        tif_filepath = None
        for root, dirs, files in os.walk(source_dir):
            for name in files:
                if name.endswith("tif") or name.endswith("tiff"):
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
        siHeader = self._parse_raw_image_info(extracted_source)
        photostim_groups = siHeader.metadata["json"]["RoiGroups"][
            "photostimRoiGroups"
        ]

        # TODO: Set default values and extract variables
        t = datetime(2022, 7, 12, 7, 00, 00)
        t2 = time(7, 00, 00)

        data_stream = Stream(
            stream_start_time=t,
            stream_end_time=t,
            stream_modalities=[Modality.POPHYS],
            camera_names=["Side Camera"],
            light_sources=[
                Laser(
                    name="Laser A",
                    wavelength=920,
                    wavelength_unit="nanometer",
                    excitation_power=int(
                        siHeader.metadata["hBeams"]["powers"][1:-1].split()[0]
                    ),
                    excitation_power_unit="percent",
                ),
            ],
            detectors=[
                Detector(name="PMT A",exposure_time=0.1, trigger_type="Internal",),
            ],
            ophys_fovs=[
                FieldOfView(
                    index=0,
                    imaging_depth=150,
                    targeted_structure="M1",
                    fov_coordinate_ml=1.5,
                    fov_coordinate_ap=1.5,
                    fov_reference="Bregma",
                    fov_width=int(
                        siHeader.metadata["hRoiManager"]["pixelsPerLine"]
                    ),
                    fov_height=int(
                        siHeader.metadata["hRoiManager"]["linesPerFrame"]
                    ),
                    magnification="16x",
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
            experimenter_full_name=["John Doe"],
            session_start_time=t,
            session_end_time=t,
            subject_id="652567",
            session_type="BCI",
            iacuc_protocol="2115",
            rig_id="Bergamo photostim.",
            data_streams=[data_stream],
            stimulus_epochs=[
                StimulusEpoch(
                    stimulus=PhotoStimulation(
                        stimulus_name="PhotoStimulation",
                        number_groups=2,
                        groups=[
                            PhotoStimulationGroup(
                                group_index=0,
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
                                number_trials=5,
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
                                group_index=0,
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
                                number_trials=5,
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
                        inter_trial_interval=10,
                    ),
                    stimulus_start_time=t2,
                    stimulus_end_time=t2,
                )
            ],
        )
