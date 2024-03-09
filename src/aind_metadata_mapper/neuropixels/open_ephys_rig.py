import typing
import logging
import pathlib
import pydantic
from xml.etree import ElementTree
from aind_data_schema.core import rig  # type: ignore

from . import neuropixels_rig, utils, NeuropixelsRigException


logger = logging.getLogger(__name__)

# the format of open_ephys settings.xml will vary based on version
SUPPORTED_SETTINGS_VERSIONS = (
    "0.6.6",
)


class ExtractedProbe(pydantic.BaseModel):

    name: str
    model: typing.Optional[str]
    serial_number: typing.Optional[str]


class ExtractContext(neuropixels_rig.NeuropixelsRigContext):

    probes: list[ExtractedProbe]


class OpenEphysRigEtl(neuropixels_rig.NeuropixelsRigEtl):

    def __init__(self, 
            input_source: pathlib.Path,
            output_directory: pathlib.Path,
            open_ephys_settings_sources: list[pathlib.Path],
            probe_manipulator_serial_numbers: typing.Optional[dict] = None,
            **kwargs
    ):
        super().__init__(input_source, output_directory, **kwargs)
        self.open_ephys_settings_sources = open_ephys_settings_sources
        self.probe_manipulator_serial_numbers = probe_manipulator_serial_numbers

    def _extract(self) -> ExtractContext:
        current = super()._extract()
        probes = []
        for source in self.open_ephys_settings_sources:
            probes.extend(self._extract_probes(
                current,
                ElementTree.fromstring(source.read_text()),
            ))
        return ExtractContext(
            current=current,
            probes=probes,
        )

    def _extract_probes(self, current: rig.Rig,
            settings: ElementTree.Element) -> list[ExtractedProbe]:
        version_elements = utils.find_elements(settings, "version")
        version = next(version_elements).text
        if version not in SUPPORTED_SETTINGS_VERSIONS:
            logger.warn(
                "Unsupported open ephys settings version: %s. Supported versions: %s"
                % (version, SUPPORTED_SETTINGS_VERSIONS, )
            )

        probes = []
        for element in utils.find_elements(settings, "np_probe"):
            probe_name = element.get("custom_probe_name")
            for ephys_assembly in current.ephys_assemblies:
                extracted_probe = None
                for probe in ephys_assembly.probes:
                    if probe.name == probe_name:
                        extracted_probe = \
                            ExtractedProbe(
                                name=probe.name,
                                model=element.get("probe_name"),
                                serial_number=element.get("probe_serial_number"),
                            )
                if extracted_probe is not None:
                    probes.append(extracted_probe)
                    break
            else:
                logger.warning(
                    "Error finding probe from open ephys settings: %s"
                    % probe_name)
                return self._infer_extracted_probes(current, settings)

        return probes

    def _infer_extracted_probes(self, current: rig.Rig,
            settings: ElementTree.Element) -> list[ExtractedProbe]:
        logger.debug(
            "Inferring associated probes from np_probe element order in open "
            "ephys settings.")
        probe_elements = list(utils.find_elements(settings, "np_probe"))
        n_probe_elements = len(probe_elements)
        n_rig_probes = sum(
            len(assembly.probes) for assembly in current.ephys_assemblies
        )
        if len(current.ephys_assemblies) != n_probe_elements:
            logger.warning(
                "Number of ephys assemblies doesnt match probes in settings. "
                "Skipping probe inference.")
            return []
        
        if n_probe_elements != n_rig_probes:
            logger.warning(
                "Number of probes in settings does not match number of probes "
                "in rig. Skipping probe inference. settings probes count: %s,"
                " rig probes count: %s" % (n_probe_elements, n_rig_probes)
            )
            return []

        probes = []
        for ephys_assembly, probe_element in \
                zip(current.ephys_assemblies, probe_elements):
            probes.append(ExtractedProbe(
                name=ephys_assembly.probes[0].name,
                model=probe_element.get("probe_name"),
                serial_number=probe_element.get("probe_serial_number"),
            ))
        return probes

    def _extract(self) -> ExtractContext:
        current = super()._extract()
        probes = []
        for source in self.open_ephys_settings_sources:
            probes.extend(self._extract_probes(
                current,
                ElementTree.fromstring(source.read_text()),
            ))
        return ExtractContext(
            current=current,
            probes=probes,
        )

    def _transform(
            self,
            extracted_source: ExtractContext) -> rig.Rig:
        # update manipulator serial numbers
        for ephys_assembly in extracted_source.current.ephys_assemblies:
            if self.probe_manipulator_serial_numbers and \
                    ephys_assembly.ephys_assembly_name in \
                    self.probe_manipulator_serial_numbers:
                ephys_assembly.manipulator.serial_number = \
                    self.probe_manipulator_serial_numbers[ephys_assembly.ephys_assembly_name]

        # update probe models and serial numbers
        for probe in extracted_source.probes:
            for ephys_assembly in extracted_source.current.ephys_assemblies:  
                try:
                    utils.find_update(
                        ephys_assembly.probes,
                        filters=[
                            ("name", probe.name),
                        ],
                        model=probe.model,
                        serial_number=probe.serial_number,
                    )
                    break
                except NeuropixelsRigException:
                    pass
            else:
                logger.warning("No probe found in rig for: %s" % probe.name)

        return super()._transform(extracted_source.current)