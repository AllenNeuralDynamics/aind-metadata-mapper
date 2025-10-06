from aind_data_schema.core.acquisition import Acquisition, DataStream
from aind_data_schema.components.configs import (
    ImagingConfig,
    Channel,
    DetectorConfig,
    LaserConfig,
    DeviceConfig,
    TriggerType,
    SampleChamberConfig,
    Immersion,
    ImageSPIM,
)
from aind_data_schema.components.coordinates import (
    CoordinateSystem,
    Axis,
    Scale,
    Translation,
)
from aind_data_schema.components.wrappers import AssetPath
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import (
    PowerUnit,
    SizeUnit,
    TimeUnit,
    AngleUnit,
)
from aind_data_schema_models.devices import ImmersionMedium
from aind_data_schema_models.coordinates import AxisName, Direction, Origin
from aind_metadata_extractor.models.smartspim import SmartspimModel
from aind_metadata_mapper.base import Mapper
from typing import List, Dict, Any, Optional


class SmartspimMapper(Mapper):
    """Smartspim Mapper"""

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
        # Extract basic information
        subject_id = metadata.slims_metadata.subject_id
        specimen_id = metadata.slims_metadata.specimen_id
        instrument_id = metadata.slims_metadata.instrument_id
        protocol_id = (
            [metadata.slims_metadata.protocol_id]
            if metadata.slims_metadata.protocol_id
            else None
        )
        experimenter_name = metadata.slims_metadata.experimenter_name

        # Build channels from wavelength config and imaging channels
        channels = self._build_channels(metadata)

        # Build ImageSPIM objects for spatial information
        images = self._build_images(metadata)

        # Build the coordinate system from axis direction information
        coordinate_system = self._build_coordinate_system(metadata)

        # Build the imaging config
        imaging_config = ImagingConfig(
            device_name=instrument_id or "SmartSPIM",
            channels=channels,
            images=images,
        )

        # Build sample chamber config if immersion data is available
        configurations = [imaging_config]
        if metadata.slims_metadata.chamber_immersion_medium:
            chamber_config = self._build_chamber_config(metadata)
            configurations.append(chamber_config)

        # Process session times with proper timezone handling
        session_start_time, session_end_time = self._process_session_times(
            metadata.file_metadata.session_start_time,
            metadata.file_metadata.session_end_time,
        )

        # Build the datastream
        data_stream = DataStream(
            stream_start_time=session_start_time,
            stream_end_time=session_end_time,
            modalities=[Modality.SPIM],
            active_devices=self._get_active_devices(metadata),
            configurations=configurations,  # type: ignore
        )

        # Validate required fields
        if not subject_id:
            raise ValueError("subject_id is required")
        if not specimen_id:
            raise ValueError("specimen_id is required")
        if not instrument_id:
            raise ValueError("instrument_id is required")

        # Build the full Acquisition model
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

        # Get imaging channels from SLIMS metadata
        imaging_channels = metadata.slims_metadata.imaging_channels
        if not imaging_channels:
            raise ValueError("imaging_channels is required")
        wavelength_config = metadata.file_metadata.wavelength_config

        for channel_name in imaging_channels:
            # Extract wavelength information
            wavelength = self._extract_wavelength_from_channel(channel_name)

            # Extract power information from wavelength config
            power, power_unit = self._extract_power_from_config(
                channel_name, wavelength_config
            )

            # Build light source config
            light_sources = []
            if wavelength:
                # Assume laser for SmartSPIM
                light_sources.append(
                    LaserConfig(
                        device_name=f"Laser_{channel_name}",
                        wavelength=wavelength,
                        wavelength_unit=SizeUnit.NM,
                        power=power,
                        power_unit=power_unit,
                    )
                )

            # Extract exposure time from tile configuration
            exposure_time = self._extract_exposure_time_from_tiles(
                channel_name, metadata
            )

            # Build detector config
            detector = DetectorConfig(
                device_name=f"Detector_{channel_name}",
                exposure_time=exposure_time,
                exposure_time_unit=TimeUnit.MS,
                trigger_type=TriggerType.INTERNAL,
            )

            # Build emission filters if available
            emission_filters, emission_wavelength = (
                self._build_emission_filters(
                    channel_name, wavelength, metadata
                )
            )

            # Build channel with proper naming convention
            channel_display_name = (
                f"Ex_{wavelength}_Em_{emission_wavelength}"
                if wavelength and emission_wavelength
                else channel_name
            )

            channel = Channel(
                channel_name=channel_display_name,
                intended_measurement=f"Channel {channel_name} signal",
                detector=detector,
                light_sources=light_sources,
                emission_filters=(
                    emission_filters if emission_filters else None
                ),
                emission_wavelength=emission_wavelength,
                emission_wavelength_unit=(
                    SizeUnit.NM if emission_wavelength else None
                ),
            )

            channels.append(channel)

        return channels

    def _build_chamber_config(
        self, metadata: SmartspimModel
    ) -> SampleChamberConfig:
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

        # Add instrument device
        if metadata.slims_metadata.instrument_id:
            devices.append(metadata.slims_metadata.instrument_id)

        # Add channel-specific devices
        imaging_channels = metadata.slims_metadata.imaging_channels
        if not imaging_channels:
            raise ValueError("imaging_channels is required")
        for channel_name in imaging_channels:
            devices.append(f"Laser_{channel_name}")
            devices.append(f"Detector_{channel_name}")

        # Add sample chamber if immersion data exists
        if metadata.slims_metadata.chamber_immersion_medium:
            devices.append("Sample Chamber")

        return devices

    def _extract_wavelength_from_channel(
        self, channel_name: str
    ) -> Optional[int]:
        """Extract wavelength from channel name."""
        # Try to parse wavelength directly from channel name
        if channel_name.isdigit():
            return int(channel_name)

        # Try to extract from Ex_XXX_Em_YYY format
        if "Ex_" in channel_name:
            parts = channel_name.split("_")
            for i, part in enumerate(parts):
                if part == "Ex" and i + 1 < len(parts):
                    try:
                        return int(parts[i + 1])
                    except ValueError:
                        continue

        return None

    def _extract_power_from_config(
        self, channel_name: str, wavelength_config: Dict[str, Any]
    ) -> tuple[Optional[float], Optional[PowerUnit]]:
        """Extract power information from wavelength config."""
        if not wavelength_config or channel_name not in wavelength_config:
            raise ValueError(
                f"wavelength_config missing for channel {channel_name}"
            )

        channel_config = wavelength_config[channel_name]
        if not isinstance(channel_config, dict):
            raise ValueError(
                f"Invalid channel_config format for {channel_name}"
            )

        # Look for power settings (left/right or general)
        power = (
            channel_config.get("power")
            or channel_config.get("power_left")
            or channel_config.get("power_right")
        )
        if power is None:
            raise ValueError(
                f"No power setting found for channel {channel_name}"
            )

        # Check for power unit specification
        power_unit_str = channel_config.get("power_unit")
        if not power_unit_str:
            raise ValueError(
                f"No power_unit specified for channel {channel_name}"
            )

        if power_unit_str.lower() in ["milliwatt", "mw"]:
            return float(power), PowerUnit.MW
        elif power_unit_str.lower() in ["microwatt", "uw"]:
            return float(power), PowerUnit.UW
        elif power_unit_str.lower() in ["percent", "%"]:
            return float(power), PowerUnit.PERCENT
        else:
            raise ValueError(
                f"Unknown power_unit '{power_unit_str}' for channel "
                f"{channel_name}"
            )

    def _build_images(self, metadata: SmartspimModel) -> List[ImageSPIM]:
        """Build ImageSPIM objects for spatial/tile information."""
        images = []

        # Get imaging channels and create images for each
        imaging_channels = metadata.slims_metadata.imaging_channels
        if not imaging_channels:
            raise ValueError("imaging_channels is required")
        filter_mapping = metadata.file_metadata.filter_mapping

        for channel_name in imaging_channels:
            wavelength = self._extract_wavelength_from_channel(channel_name)
            emission_wavelength = None

            if filter_mapping and str(wavelength) in filter_mapping:
                emission_wavelength = filter_mapping[str(wavelength)]

            # Create filename pattern based on channel
            filename = (
                f"Ex_{wavelength}_Em_{emission_wavelength}.ims"
                if wavelength and emission_wavelength
                else f"{channel_name}.ims"
            )

            # Build channel display name
            channel_display_name = (
                f"Ex_{wavelength}_Em_{emission_wavelength}"
                if wavelength and emission_wavelength
                else channel_name
            )

            # Create basic transform (identity for now)
            image_transform = [
                Scale(scale=[1.0, 1.0, 1.0]),
                Translation(translation=[0.0, 0.0, 0.0]),
            ]

            image = ImageSPIM(
                channel_name=channel_display_name,
                file_name=AssetPath(filename),
                imaging_angle=0,  # Default angle
                imaging_angle_unit=AngleUnit.DEG,
                image_start_time=metadata.file_metadata.session_start_time,
                image_end_time=metadata.file_metadata.session_end_time,
                image_to_acquisition_transform=image_transform,
                dimensions_unit=SizeUnit.PX,
            )

            images.append(image)

        return images

    def _build_coordinate_system(
        self, metadata: SmartspimModel
    ) -> Optional[CoordinateSystem]:
        """Build CoordinateSystem from axis direction info in SLIMS."""
        slims = metadata.slims_metadata

        # Check if we have the required direction information
        if not all([slims.x_direction, slims.y_direction, slims.z_direction]):
            return None

        # Map direction strings to Direction enums
        # Support both underscore and space formats for compatibility
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

        # Map direction enums to coordinate system letters
        direction_to_letter = {
            Direction.LR: "R",
            Direction.RL: "L",
            Direction.AP: "P",
            Direction.PA: "A",
            Direction.SI: "I",
            Direction.IS: "S",
        }

        # Build axes from the direction information
        axes = []
        direction_letters = []

        try:
            # X axis
            if slims.x_direction is None:
                raise ValueError("X direction is None")
            x_direction = direction_mapping.get(slims.x_direction)
            if not x_direction:
                raise ValueError(f"Invalid X direction: {slims.x_direction}")
            axes.append(Axis(name=AxisName.X, direction=x_direction))
            direction_letters.append(direction_to_letter[x_direction])

            # Y axis
            if slims.y_direction is None:
                raise ValueError("Y direction is None")
            y_direction = direction_mapping.get(slims.y_direction)
            if not y_direction:
                raise ValueError(f"Invalid Y direction: {slims.y_direction}")
            axes.append(Axis(name=AxisName.Y, direction=y_direction))
            direction_letters.append(direction_to_letter[y_direction])

            # Z axis
            if slims.z_direction is None:
                raise ValueError("Z direction is None")
            z_direction = direction_mapping.get(slims.z_direction)
            if not z_direction:
                raise ValueError(f"Invalid Z direction: {slims.z_direction}")
            axes.append(Axis(name=AxisName.Z, direction=z_direction))
            direction_letters.append(direction_to_letter[z_direction])

        except (ValueError, KeyError) as e:
            print(f"Invalid axes configuration: {e}")
            return None

        # Create coordinate system name based on anatomical directions
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
        for enum_member in ImmersionMedium:
            if medium_lower == enum_member.value.lower():
                return enum_member

        return ImmersionMedium.OTHER

    def _extract_exposure_time_from_tiles(
        self, channel_name: str, metadata: SmartspimModel
    ) -> float:
        """Extract exposure time for a channel from tile configuration."""
        tile_config = metadata.file_metadata.tile_config
        if not tile_config:
            raise ValueError(
                "tile_config is required for exposure time extraction"
            )

        # Look for tiles with the matching laser/channel
        wavelength = self._extract_wavelength_from_channel(channel_name)
        if not wavelength:
            raise ValueError(
                f"Cannot extract wavelength from channel {channel_name}"
            )

        # Find tiles that use this wavelength
        for tile_key, tile_info in tile_config.items():
            if isinstance(tile_info, dict) and str(
                tile_info.get("Laser")
            ) == str(wavelength):
                exposure = tile_info.get("Exposure")
                if exposure is not None:
                    try:
                        return float(exposure)
                    except (ValueError, TypeError):
                        continue

        raise ValueError(
            f"No exposure time found for wavelength {wavelength} "
            f"in tile_config"
        )

    def _build_emission_filters(
        self,
        channel_name: str,
        wavelength: Optional[int],
        metadata: SmartspimModel,
    ) -> tuple[List[DeviceConfig], Optional[int]]:
        """Build emission filters for a channel."""
        emission_filters = []
        emission_wavelength = None

        # Use filter mapping from file metadata
        filter_mapping = metadata.file_metadata.filter_mapping
        if filter_mapping and wavelength and str(wavelength) in filter_mapping:
            emission_wavelength = filter_mapping[str(wavelength)]
            filter_name = f"Filter_{emission_wavelength}"
            emission_filters.append(DeviceConfig(device_name=filter_name))

        # Additional logic could be added here to extract filter info
        # from tile configuration if needed

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
