"""Smartspim mapper"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from aind_data_schema.components.configs import (
    Channel,
    DetectorConfig,
    DeviceConfig,
    ImageSPIM,
    ImagingConfig,
    Immersion,
    LaserConfig,
    SampleChamberConfig,
    TriggerType,
)
from aind_data_schema.components.coordinates import Axis, CoordinateSystem, Scale, Translation
from aind_data_schema.components.identifiers import Code
from aind_data_schema.components.wrappers import AssetPath
from aind_data_schema.core.acquisition import Acquisition, DataStream
from aind_data_schema_models.coordinates import AxisName, Direction, Origin
from aind_data_schema_models.devices import ImmersionMedium
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import AngleUnit, PowerUnit, SizeUnit, TimeUnit
from aind_metadata_extractor.models.smartspim import SmartspimModel

from aind_metadata_mapper.base import MapperJob, MapperJobSettings


class SmartspimMapper(MapperJob):
    """Smartspim Mapper"""

    def run_job(self, job_settings: MapperJobSettings):
        """Load the metadata input file and transform to Acquisition model."""

        with open(job_settings.input_filepath, "r") as f:
            raw_metadata = json.load(f)

        smartspim_metadata = SmartspimModel.model_validate(raw_metadata)
        acquisition = self._transform(smartspim_metadata)

        # Pull apart the target directory and suffix from output filepath
        output_path = str(job_settings.output_filepath)
        directory = output_path.rsplit("/", 1)[0] + "/"
        filename = output_path.rsplit("/", 1)[1]
        suffix = "_" + filename.split("_", 1)[1].rsplit(".", 1)[0]

        acquisition.write_standard_file(output_directory=Path(directory), suffix=suffix)

    def transform(self, metadata: dict) -> Acquisition:
        """Transforms raw metadata into a complete model."""
        smartspim_metadata = SmartspimModel.model_validate(metadata)
        return self._transform(smartspim_metadata)

    def _transform(self, metadata: SmartspimModel) -> Acquisition:
        """
        Transforms raw metadata from both microscope files and SLIMS
        into a complete Acquisition model.

        Parameters
        ----------
        metadata : SmartspimModel
            Metadata extracted from the microscope files and SLIMS service.

        Returns
        -------
        Acquisition
            Fully composed acquisition model.
        """
        subject_id = cast(str, metadata.slims_metadata.subject_id)
        raw_specimen_id = cast(str, metadata.slims_metadata.specimen_id)
        instrument_id = cast(str, metadata.slims_metadata.instrument_id)

        if subject_id not in raw_specimen_id:
            specimen_id = f"{subject_id}-{raw_specimen_id}"
        else:
            specimen_id = raw_specimen_id

        protocol_id = [metadata.slims_metadata.protocol_id] if metadata.slims_metadata.protocol_id else None

        experimenter_name = metadata.slims_metadata.experimenter_name or metadata.slims_metadata.order_created_by

        channels = self._build_channels(metadata)
        images = self._build_images(metadata)
        coordinate_system = self._build_coordinate_system(metadata)

        imaging_config = ImagingConfig(
            device_name=instrument_id,
            channels=channels,
            images=cast(Any, images),
            coordinate_system=coordinate_system,
        )

        configurations: List[Any] = [imaging_config]
        if metadata.slims_metadata.chamber_immersion_medium:
            chamber_config = self._build_chamber_config(metadata)
            configurations.append(chamber_config)

        session_start_time, session_end_time = self._process_session_times(
            metadata.file_metadata.session_start_time,
            metadata.file_metadata.session_end_time,
        )

        software_version = metadata.file_metadata.session_config.get("Version")
        code_list = (
            [
                Code(
                    name="SmartSPIM Acquisition Software",
                    version=software_version,
                    url="https://github.com/AllenNeuralDynamics/smartspim-acquisition",
                )
            ]
            if software_version
            else None
        )

        data_stream = DataStream(
            stream_start_time=session_start_time,
            stream_end_time=session_end_time,
            modalities=[Modality.SPIM],
            code=code_list,
            active_devices=self._get_active_devices(metadata),
            configurations=configurations,  # type: ignore
        )

        acquisition = Acquisition(
            subject_id=subject_id,
            specimen_id=specimen_id,
            acquisition_start_time=session_start_time,
            acquisition_end_time=session_end_time,
            experimenters=[experimenter_name] if experimenter_name else [],
            protocol_id=protocol_id,
            instrument_id=instrument_id,
            acquisition_type="SmartSPIM",
            coordinate_system=coordinate_system,
            data_streams=[data_stream],
            stimulus_epochs=[],
        )

        return acquisition

    def _build_channels(self, metadata: SmartspimModel) -> List[Channel]:
        """Build Channel objects from wavelength and imaging channels."""
        channels = []

        imaging_channels = cast(List[str], metadata.slims_metadata.imaging_channels)
        wavelength_config = metadata.file_metadata.wavelength_config
        laser_powers = self._extract_laser_powers_from_tiles(metadata)

        for channel_name in imaging_channels:
            wavelength = self._extract_wavelength_from_channel(channel_name)
            light_sources = self._build_light_sources(wavelength, wavelength_config, laser_powers)
            exposure_time = self._extract_exposure_time_from_tiles(channel_name, metadata)

            detector = DetectorConfig(
                device_name="Camera",
                exposure_time=exposure_time,
                exposure_time_unit=TimeUnit.MS,
                trigger_type=TriggerType.INTERNAL,
            )

            emission_filters, emission_wavelength = self._build_emission_filters(channel_name, wavelength, metadata)
            channel_display_name = self._get_channel_display_name(channel_name, wavelength, metadata)

            channel = Channel(
                channel_name=channel_display_name,
                intended_measurement=None,
                detector=detector,
                light_sources=cast(Any, light_sources),
                emission_filters=(emission_filters if emission_filters else None),
                emission_wavelength=emission_wavelength,
                emission_wavelength_unit=(SizeUnit.NM if emission_wavelength else None),
            )

            channels.append(channel)

        return channels

    def _build_light_sources(
        self,
        wavelength: int,
        wavelength_config: Dict[str, Any],
        laser_powers: Dict[int, Dict[str, float]],
    ) -> List[LaserConfig]:
        """Build LaserConfig objects for light sources."""
        light_sources = []

        wavelength_key = str(wavelength)
        channel_config = wavelength_config[wavelength_key]
        power_unit = self._parse_power_unit(channel_config.get("power_unit", "percent"))

        if wavelength in laser_powers:
            tile_powers = laser_powers[wavelength]
            if "left" in tile_powers:
                light_sources.append(
                    LaserConfig(
                        device_name=f"Ex_{wavelength}",
                        wavelength=wavelength,
                        wavelength_unit=SizeUnit.NM,
                        power=tile_powers["left"],
                        power_unit=power_unit,
                    )
                )

            if "right" in tile_powers:
                light_sources.append(
                    LaserConfig(
                        device_name=f"Ex_{wavelength}",
                        wavelength=wavelength,
                        wavelength_unit=SizeUnit.NM,
                        power=tile_powers["right"],
                        power_unit=power_unit,
                    )
                )
        else:
            power_left = channel_config.get("power_left")
            if power_left:
                light_sources.append(
                    LaserConfig(
                        device_name=f"Ex_{wavelength}",
                        wavelength=wavelength,
                        wavelength_unit=SizeUnit.NM,
                        power=float(power_left),
                        power_unit=power_unit,
                    )
                )

            power_right = channel_config.get("power_right")
            if power_right:
                light_sources.append(
                    LaserConfig(
                        device_name=f"Ex_{wavelength}",
                        wavelength=wavelength,
                        wavelength_unit=SizeUnit.NM,
                        power=float(power_right),
                        power_unit=power_unit,
                    )
                )

        return light_sources

    def _parse_power_unit(self, power_unit_str: str) -> PowerUnit:
        """Parse power unit string to PowerUnit enum."""
        if power_unit_str.lower() in ["milliwatt", "mw"]:
            return PowerUnit.MW
        elif power_unit_str.lower() in ["microwatt", "uw"]:
            return PowerUnit.UW
        else:
            return PowerUnit.PERCENT

    def _build_chamber_config(self, metadata: SmartspimModel) -> SampleChamberConfig:
        """Build SampleChamberConfig from immersion metadata."""
        # Parse chamber immersion data
        chamber_medium = metadata.slims_metadata.chamber_immersion_medium
        chamber_ri = metadata.slims_metadata.chamber_refractive_index

        chamber_immersion = Immersion(
            medium=self._map_immersion_medium(chamber_medium),
            refractive_index=float(chamber_ri) if chamber_ri else 1.0,
        )

        # Parse sample immersion data if available
        sample_immersion = None
        if metadata.slims_metadata.sample_immersion_medium:
            sample_medium = metadata.slims_metadata.sample_immersion_medium
            sample_ri = metadata.slims_metadata.sample_refractive_index
            sample_immersion = Immersion(
                medium=self._map_immersion_medium(sample_medium),
                refractive_index=float(sample_ri) if sample_ri else 1.0,
            )

        return SampleChamberConfig(
            device_name="Sample Chamber",
            chamber_immersion=chamber_immersion,
            sample_immersion=sample_immersion,
        )

    def _get_active_devices(self, metadata: SmartspimModel) -> List[str]:
        """Get list of active device names."""
        devices = []

        if metadata.slims_metadata.instrument_id:
            devices.append(metadata.slims_metadata.instrument_id)

        devices.append("Camera")

        imaging_channels = cast(List[str], metadata.slims_metadata.imaging_channels)
        wavelengths_used = set()
        for channel_name in imaging_channels:
            wavelength = self._extract_wavelength_from_channel(channel_name)
            wavelengths_used.add(wavelength)

        for wavelength in sorted(wavelengths_used):
            devices.append(f"Ex_{wavelength}")

        if metadata.slims_metadata.chamber_immersion_medium:
            devices.append("Sample Chamber")

        return devices

    def _extract_wavelength_from_channel(self, channel_name: str) -> int:
        """Extract wavelength from channel name (e.g., 'Laser = 488; ...')."""
        parts = channel_name.split(";")[0]
        laser_part = parts.split("=")[-1].strip()
        return int(laser_part)

    def _get_channel_display_name(self, channel_name: str, wavelength: int, metadata: SmartspimModel) -> str:
        """Get consistent channel display name."""
        _, emission_wavelength = self._build_emission_filters(channel_name, wavelength, metadata)
        return f"Ex_{wavelength}_Em_{emission_wavelength}"

    def _get_channel_display_name_from_wavelength(self, wavelength: int, metadata: SmartspimModel) -> str:
        """Get consistent channel display name from wavelength only."""
        filter_mapping = metadata.file_metadata.filter_mapping
        if filter_mapping and str(wavelength) in filter_mapping:
            emission_wavelength = filter_mapping[str(wavelength)]
            return f"Ex_{wavelength}_Em_{emission_wavelength}"
        return f"Ex_{wavelength}"

    def _build_images(self, metadata: SmartspimModel) -> List[ImageSPIM]:
        """Build ImageSPIM objects for spatial/tile information."""
        images = []

        session_config = metadata.file_metadata.session_config
        pixel_size_um = cast(str, session_config.get("um/pix"))
        pixel_size = float(pixel_size_um)

        tile_config = metadata.file_metadata.tile_config

        for tile_key, tile_info in tile_config.items():
            wavelength = int(tile_info.get("Laser"))
            channel_display_name = self._get_channel_display_name_from_wavelength(wavelength, metadata)

            tile_x = float(tile_info.get("X"))
            tile_y = float(tile_info.get("Y"))
            tile_z = float(tile_info.get("Z"))

            filename = f"{channel_display_name}/{int(tile_x)}/{int(tile_x)}_{int(tile_y)}/"

            image_transform = [
                Scale(scale=[pixel_size, pixel_size, pixel_size]),
                Translation(translation=[tile_x, tile_y, tile_z]),
            ]

            image = ImageSPIM(
                channel_name=channel_display_name,
                file_name=AssetPath(filename),
                imaging_angle=0,
                imaging_angle_unit=AngleUnit.DEG,
                image_start_time=metadata.file_metadata.session_start_time,
                image_end_time=metadata.file_metadata.session_end_time,
                image_to_acquisition_transform=image_transform,
                dimensions_unit=SizeUnit.PX,
            )

            images.append(image)

        return images

    def _build_coordinate_system(self, metadata: SmartspimModel) -> Optional[CoordinateSystem]:
        """Build CoordinateSystem from axis direction info in SLIMS."""
        slims = metadata.slims_metadata

        if not all([slims.x_direction, slims.y_direction, slims.z_direction]):
            return None

        direction_mapping = {
            "Left to Right": Direction.LR,
            "Left_to_right": Direction.LR,
            "Right to Left": Direction.RL,
            "Right_to_left": Direction.RL,
            "Anterior to Posterior": Direction.AP,
            "Anterior_to_posterior": Direction.AP,
            "Posterior to Anterior": Direction.PA,
            "Posterior_to_anterior": Direction.PA,
            "Superior to Inferior": Direction.SI,
            "Superior_to_inferior": Direction.SI,
            "Inferior to Superior": Direction.IS,
            "Inferior_to_superior": Direction.IS,
        }

        direction_to_letter = {
            Direction.LR: "R",
            Direction.RL: "L",
            Direction.AP: "P",
            Direction.PA: "A",
            Direction.SI: "I",
            Direction.IS: "S",
        }

        axes = []
        direction_letters = []

        x_direction = direction_mapping[cast(str, slims.x_direction)]
        axes.append(Axis(name=AxisName.X, direction=x_direction))
        direction_letters.append(direction_to_letter[x_direction])

        y_direction = direction_mapping[cast(str, slims.y_direction)]
        axes.append(Axis(name=AxisName.Y, direction=y_direction))
        direction_letters.append(direction_to_letter[y_direction])

        z_direction = direction_mapping[cast(str, slims.z_direction)]
        axes.append(Axis(name=AxisName.Z, direction=z_direction))
        direction_letters.append(direction_to_letter[z_direction])

        coordinate_system_name = f"SPIM_{''.join(direction_letters)}"

        return CoordinateSystem(
            name=coordinate_system_name,
            origin=Origin.ORIGIN,
            axes=axes,
            axis_unit=SizeUnit.UM,
        )

    def _map_immersion_medium(self, medium: Optional[str]) -> ImmersionMedium:
        """Map SLIMS immersion medium to schema enum."""
        if not medium:
            return ImmersionMedium.AIR

        medium_lower = medium.lower().strip()

        # Comprehensive medium mapping based on upgrader patterns
        medium_mappings = {
            "cargille 1.52": ImmersionMedium.OIL,
            "cargille 1.5200": ImmersionMedium.OIL,
            "cargille oil 1.5200": ImmersionMedium.OIL,
            "cargille oil 1.52": ImmersionMedium.OIL,
            "cargille oil": ImmersionMedium.OIL,
            "easyindex": ImmersionMedium.EASYINDEX,
            "0.05x ssc": ImmersionMedium.WATER,
            "acb": ImmersionMedium.ACB,
            "ethyl cinnamate": ImmersionMedium.ECI,
            "dih2o": ImmersionMedium.WATER,
            "water": ImmersionMedium.WATER,
        }

        # First check for exact matches
        if medium_lower in medium_mappings:
            return medium_mappings[medium_lower]

        # Then check for partial matches
        for key, value in medium_mappings.items():
            if key in medium_lower:
                return value

        # Try to match against enum values (case-insensitive)
        for enum_member in ImmersionMedium:  # pragma: no cover
            if medium_lower == enum_member.value.lower():
                return enum_member

        return ImmersionMedium.OTHER  # pragma: no cover

    def _extract_exposure_time_from_tiles(self, channel_name: str, metadata: SmartspimModel) -> float:
        """Extract exposure time for a channel from tile configuration."""
        tile_config = metadata.file_metadata.tile_config
        wavelength = self._extract_wavelength_from_channel(channel_name)

        for tile_key, tile_info in tile_config.items():
            if str(tile_info.get("Laser")) == str(wavelength):
                return float(tile_info.get("Exposure"))

        raise ValueError(f"No exposure time found for wavelength {wavelength}")

    def _extract_laser_powers_from_tiles(self, metadata: SmartspimModel) -> Dict[int, Dict[str, float]]:
        """
        Extract actual laser powers used in tiles.

        Returns a dict mapping wavelength to a dict of side -> power.
        E.g., {488: {'left': 25.0, 'right': 30.0}}
        """
        tile_config = metadata.file_metadata.tile_config
        wavelength_config = metadata.file_metadata.wavelength_config

        side_map = {"0": "left", "1": "right"}
        laser_powers = {}

        for tile_key, tile_info in tile_config.items():
            wavelength = int(tile_info.get("Laser"))
            side = str(tile_info.get("Side"))
            side_str = side_map[side]

            if wavelength not in laser_powers:
                laser_powers[wavelength] = {}

            power_key = f"power_{side_str}"
            wavelength_entry = wavelength_config[str(wavelength)]
            laser_powers[wavelength][side_str] = float(wavelength_entry[power_key])

        return laser_powers

    def _build_emission_filters(
        self,
        channel_name: str,
        wavelength: int,
        metadata: SmartspimModel,
    ) -> tuple[List[DeviceConfig], int]:
        """Build emission filters for a channel."""
        filter_mapping = metadata.file_metadata.filter_mapping
        emission_wavelength = filter_mapping[str(wavelength)]
        emission_filters = [DeviceConfig(device_name=f"Em_{emission_wavelength}")]

        return emission_filters, emission_wavelength

    def _process_session_times(self, session_start_time, session_end_time):
        """Process and validate session times with Pacific timezone."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        # Pacific timezone - automatically handles PST/PDT transitions
        pacific_tz = ZoneInfo("America/Los_Angeles")

        def ensure_pacific_timezone(dt):
            """Ensure datetime is in Pacific timezone"""
            if dt is None:
                # Return current time as fallback
                return datetime.now(pacific_tz)
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pacific_tz)
            return dt

        # Convert start and end times and ensure Pacific timezone
        session_start_time = ensure_pacific_timezone(session_start_time)
        session_end_time = ensure_pacific_timezone(session_end_time)

        # Invert start/end time if they are in the wrong order
        if session_start_time > session_end_time:
            session_start_time, session_end_time = (
                session_end_time,
                session_start_time,
            )

        return session_start_time, session_end_time
