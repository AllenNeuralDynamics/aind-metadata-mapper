"""ETL for the MVR config."""

import logging
from pathlib import Path
from typing import Dict, List, Tuple

from aind_data_schema.components.devices import Software  # type: ignore
from aind_data_schema.core.rig import Rig  # type: ignore

from aind_metadata_mapper.dynamic_routing import utils
from aind_metadata_mapper.dynamic_routing.neuropixels_rig import (
    NeuropixelsRigContext,
    NeuropixelsRigEtl,
)

logger = logging.getLogger(__name__)


class ExtractContext(NeuropixelsRigContext):
    """Extract context for MVR rig etl."""

    serial_numbers: List[Tuple[str, str]]


class MvrRigEtl(NeuropixelsRigEtl):
    """MVR rig ETL class. Extracts information from MVR-related config file."""

    def __init__(
        self,
        input_source: Path,
        output_directory: Path,
        mvr_config_source: Path,
        mvr_mapping: Dict[str, str],
        **kwargs,
    ):
        """Class constructor for MVR rig etl class."""
        super().__init__(input_source, output_directory, **kwargs)
        self.mvr_mapping = mvr_mapping
        self.mvr_config_source = mvr_config_source

    def _extract(self) -> ExtractContext:
        """Extracts MVR-related camera information from config file."""
        mvr_config = utils.load_config(self.mvr_config_source)
        serial_numbers = []
        for mvr_name, assembly_name in self.mvr_mapping.items():
            try:
                mvr_camera_config = mvr_config[mvr_name]
            except KeyError:
                logger.debug(
                    "No camera found for: %s in mvr config." % mvr_name
                )
                continue
            serial_numbers.append(
                (
                    assembly_name,
                    "".join(mvr_camera_config["sn"]),
                )
            )
        return ExtractContext(
            current=super()._extract(),
            serial_numbers=serial_numbers,
        )

    def _transform(self, extracted_source: ExtractContext) -> Rig:
        """Updates rig model with MVR-related camera information."""
        for assembly_name, serial_number in extracted_source.serial_numbers:
            utils.find_update(
                extracted_source.current.cameras,
                filters=[
                    (
                        "name",
                        assembly_name,
                    ),
                ],
                setter=(
                    lambda item, name, value: setattr(item.camera, name, value)
                ),
                serial_number=serial_number,
                recording_software=Software(
                    name="MVR",
                    version="Not detected/provided.",
                ),
            )

        return super()._transform(extracted_source.current)
