"""ETL for the Open Ephys config."""

import logging
from pathlib import Path
from typing import List, Optional, Tuple
from xml.etree import ElementTree

from aind_data_schema.core.rig import Rig  # type: ignore
from pydantic import BaseModel

from aind_metadata_mapper.dynamic_routing import utils
from aind_metadata_mapper.dynamic_routing.neuropixels_rig import (
    NeuropixelsRigContext,
    NeuropixelsRigEtl,
)

logger = logging.getLogger(__name__)


class ExtractedProbe(BaseModel):
    """Extracted probe information."""

    name: Optional[str]
    model: Optional[str]
    serial_number: Optional[str]


class ExtractContext(NeuropixelsRigContext):
    """Extract context for Open Ephys rig etl."""

    probes: List[ExtractedProbe]
    versions: List[Optional[str]]


class OpenEphysRigEtl(NeuropixelsRigEtl):
    """Open Ephys rig ETL class. Extracts information from Open Ephys-related
    config files."""

    def __init__(
        self,
        input_source: Path,
        output_directory: Path,
        open_ephys_settings_sources: List[Path],
        probe_manipulator_serial_numbers: List[Tuple[str, str]] = [],
        **kwargs,
    ):
        """Class constructor for Open Ephys rig etl class."""
        super().__init__(input_source, output_directory, **kwargs)
        self.open_ephys_settings_sources = open_ephys_settings_sources
        self.probe_manipulator_serial_numbers = (
            probe_manipulator_serial_numbers
        )

    def _extract(self) -> ExtractContext:
        """Extracts Open Ephys-related probe information from config files."""
        current = super()._extract()
        versions = []
        probes = []
        for source in self.open_ephys_settings_sources:
            parsed = utils.load_xml(source)
            versions.append(self._extract_version(parsed))
            probes.extend(
                self._extract_probes(
                    current,
                    parsed,
                )
            )
        return ExtractContext(
            current=current,
            probes=probes,
            versions=versions,
        )

    @staticmethod
    def _extract_version(settings: ElementTree.Element) -> Optional[str]:
        """Extracts the version from the Open Ephys settings file."""
        version_elements = utils.find_elements(settings, "version")
        return next(version_elements).text

    @staticmethod
    def _extract_probes(
        current: Rig, settings: ElementTree.Element
    ) -> List[ExtractedProbe]:
        """Extracts probe serial numbers from Open Ephys settings file. If
         extracted probe names do not match the rig, attempt to infer them from
        the current rig model.
        """
        extracted_probes = [
            ExtractedProbe(
                name=element.get("custom_probe_name"),
                model=element.get("probe_name"),
                serial_number=element.get("probe_serial_number"),
            )
            for element in utils.find_elements(settings, "np_probe")
        ]
        # if extracted probe names are not in the rig, attempt to infer them
        # from current rig model
        extracted_probe_names = [probe.name for probe in extracted_probes]
        rig_probe_names = [
            probe.name
            for assembly in current.ephys_assemblies
            for probe in assembly.probes
        ]
        if not all(name in rig_probe_names for name in extracted_probe_names):
            logger.warning(
                "Mismatched probe names in open ephys settings. Attempting to "
                "infer probe names. extracted: %s, rig: %s"
                % (extracted_probe_names, rig_probe_names)
            )
            if len(extracted_probe_names) != len(rig_probe_names):
                logger.warning(
                    "Probe count mismatch. Skipping probe inference."
                )
                return []
            for extracted_probe, rig_probe_name in zip(
                extracted_probes, rig_probe_names
            ):
                extracted_probe.name = rig_probe_name

        return extracted_probes

    def _transform(
        self,
        extracted_source: ExtractContext,
    ) -> Rig:
        """Updates rig model with Open Ephys-related probe information."""
        # update manipulator serial numbers
        for (
            ephys_assembly_name,
            serial_number,
        ) in self.probe_manipulator_serial_numbers:
            utils.find_update(
                extracted_source.current.ephys_assemblies,
                [
                    ("name", ephys_assembly_name),
                ],
                setter=(
                    lambda item, name, value: setattr(
                        item.manipulator, name, value
                    )
                ),
                serial_number=serial_number,
            )

        # update probe models and serial numbers
        for probe in extracted_source.probes:
            for ephys_assembly in extracted_source.current.ephys_assemblies:
                updated = utils.find_update(
                    ephys_assembly.probes,
                    filters=[
                        ("name", probe.name),
                    ],
                    model=probe.model,
                    serial_number=probe.serial_number,
                )
                if updated:
                    break

        return super()._transform(extracted_source.current)
