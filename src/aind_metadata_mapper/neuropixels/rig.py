"""ETL class for neuropixels rigs."""

import json
import yaml
import pathlib
import datetime
import configparser
from aind_data_schema import rig

from ..core import BaseEtl


class NeuropixelsRigException(Exception):
    """General error for MVR."""


# assembly name, serial number, height, width, frame rate
MVRCamera = tuple[str, str, int, int, float]
SyncChannel = tuple[int, str]  # di line index, name
# device name, sample rate, channels
SyncContext = tuple[str, float, list[SyncChannel]]
RigContext = tuple[dict, list[MVRCamera], SyncContext]


class NeuropixelsRigEtl(BaseEtl):
    """Neuropixels rig ETL class. Extracts information from rig-related files
    and transforms them into an aind-data-schema rig.Rig instance.
    """

    def __init__(
        self,
        input_source: pathlib.Path,
        output_directory: pathlib.Path,
    ):
        """Class constructor for Neuropixels rig etl class.

        Parameters
        ----------
        input_source : Path
          Can be a string or a Path
        output_directory : Path
          The directory where to save the json files.
        """
        super().__init__(input_source, output_directory)

    def _load_resource(self, path: pathlib.Path) -> str:
        """Loads a rig-related resource from input source.
        """
        if not path.exists():
            raise NeuropixelsRigException("%s not found." % path)

        return path.read_text()

    def _extract(self) -> RigContext:
        """Extracts rig-related information from config files.
        """
        if not self.input_source.is_dir():
            raise NeuropixelsRigException(
                "Input source is not a directory. %s" % self.input_source
            )

        return (
            json.loads(
                self._load_resource(
                    self.input_source / "rig.partial.json",
                )
            ),
            self._extract_mvr(
                self._load_resource(
                    self.input_source / "mvr.ini",
                ),
                json.loads(
                    self._load_resource(
                        self.input_source / "mvr.mapping.json",
                    )
                ),
            ),
            self._extract_sync(
                self._load_resource(
                    self.input_source / "sync.yml",
                )
            ),
        )

    def _extract_mvr(self, content: str, mapping: dict) -> list[MVRCamera]:
        """Extracts camera-related information from MPE mvr config.
        """
        config = configparser.ConfigParser()
        config.read_string(content)
        frame_rate = float(
            config["CAMERA_DEFAULT_CONFIG"]["frames_per_second"]
        )
        height = int(config["CAMERA_DEFAULT_CONFIG"]["height"])
        width = int(config["CAMERA_DEFAULT_CONFIG"]["width"])

        extracted = []
        for mvr_name, assembly_name in mapping.items():
            try:
                extracted.append(
                    (
                        assembly_name,
                        "".join(config[mvr_name]["sn"]),
                        height,
                        width,
                        frame_rate,
                    )
                )
            except KeyError:
                raise NeuropixelsRigException(
                    "No camera found for: %s in mvr.ini" %
                    mvr_name
                )
        return extracted

    def _extract_sync(self, content: str) -> SyncContext:
        """Extracts DAQ-related information from MPE sync config.
        """
        config = yaml.safe_load(content)
        return (
            config["device"],
            config["freq"],
            list(config["line_labels"].items()),
        )

    def _transform(self, extracted_source: RigContext) -> rig.Rig:
        """Transforms extracted rig context into aind-data-schema rig.Rig
        instance.
        """
        partial, mvr_cameras, sync_context = extracted_source

        # search for partial sync daq
        sync_device_name, sync_sample_rate, sync_channels = sync_context
        sync_daq_name = "Sync"
        for idx, partial_daq in enumerate(partial["daqs"]):
            if partial_daq["name"] == sync_daq_name:
                # remove from daqs for later spread operation
                partial_sync_daq = partial["daqs"].pop(idx)
                break
        else:
            raise NeuropixelsRigException(
                "Sync daq not found in partial rig. expected=%s" %
                sync_daq_name
            )

        sync_daq = {
            **partial_sync_daq,
            "channels": [
                {
                    "channel_name": name,
                    "channel_type": "Digital Input",
                    "device_name": sync_device_name,
                    "event_based_sampling": False,
                    "channel_index": line,
                    "sample_rate": sync_sample_rate,
                    "sample_rate_unit": "hertz"
                }
                for line, name in sync_channels
            ],
        }

        camera_assemblies = []
        for partial_camera_assembly in partial["cameras"]:
            name = partial_camera_assembly["camera_assembly_name"]

            found = list(filter(
                lambda camera: camera[0] == name,
                mvr_cameras,
            ))
            if len(found) < 1:
                raise NeuropixelsRigException(
                    "Camera assembly not found in partial rig. expected=%s"
                    % name
                )

            assembly_name, serial_number, height, width, frame_rate = found[0]
            camera_assemblies.append({
                **partial_camera_assembly,
                "camera": {
                    **partial_camera_assembly["camera"],
                    "name": assembly_name,
                    "serial_number": serial_number,
                    "pixel_height": height,
                    "pixel_width": width,
                    "size_unit": "pixel",
                    "max_frame_rate": frame_rate,
                }
            })

        return rig.Rig.parse_obj({
            **partial,
            "modification_date": datetime.date.today(),
            "daqs": [
                *partial["daqs"],
                sync_daq,
            ],
            "cameras": camera_assemblies,
        })
