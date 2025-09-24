from aind_data_schema.core.acquisition import Acquisition, DataStream
from aind_data_schema.components.configs import (
    ImagingConfig,
    Channel,
    DetectorConfig,
    LightEmittingDiodeConfig,
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
from aind_metadata_mapper.models import Mapper
from typing import List, Dict, Any, Optional


class SmartspimMapper(Mapper):
    """Smartspim Mapper"""
    
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
        
        # Build the imaging config
        imaging_config = ImagingConfig(
            device_name=instrument_id or "SmartSPIM",
            channels=channels,
            images=images
        )
        
        # Build sample chamber config if immersion data is available
        configurations = [imaging_config]
        if metadata.slims_metadata.chamber_immersion_medium:
            chamber_config = self._build_chamber_config(metadata)
            configurations.append(chamber_config)
        
        # Build the datastream
        data_stream = DataStream(
            stream_start_time=metadata.file_metadata.session_start_time,
            stream_end_time=metadata.file_metadata.session_end_time,
            modalities=[Modality.SPIM],
            active_devices=self._get_active_devices(metadata),
            configurations=configurations
        )
        
        # Build the full Acquisition model
        acquisition = Acquisition(
            subject_id=subject_id,
            specimen_id=specimen_id,
            acquisition_start_time=metadata.file_metadata.session_start_time,
            acquisition_end_time=metadata.file_metadata.session_end_time,
            experimenters=[experimenter_name] if experimenter_name else [],
            protocol_id=protocol_id,
            instrument_id=instrument_id,
            acquisition_type="SmartSPIM",
            data_streams=[data_stream],
            stimulus_epochs=[]
        )

        return acquisition
    
    def _build_channels(self, metadata: SmartspimModel) -> List[Channel]:
        """Build Channel objects from wavelength and imaging channels."""
        channels = []
        
        # Get imaging channels from SLIMS metadata
        imaging_channels = metadata.slims_metadata.imaging_channels or []
        wavelength_config = metadata.file_metadata.wavelength_config
        filter_mapping = metadata.file_metadata.filter_mapping
        
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
                light_sources.append(LaserConfig(
                    device_name=f"Laser_{channel_name}",
                    wavelength=wavelength,
                    wavelength_unit=SizeUnit.NM,
                    power=power,
                    power_unit=power_unit
                ))
            
            # Build detector config
            detector = DetectorConfig(
                device_name=f"Detector_{channel_name}",
                exposure_time=1.0,  # Default exposure time
                exposure_time_unit=TimeUnit.MS,
                trigger_type=TriggerType.INTERNAL
            )
            
            # Build emission filters if available
            emission_filters = []
            emission_wavelength = None
            if filter_mapping and str(wavelength) in filter_mapping:
                emission_wavelength = filter_mapping[str(wavelength)]
                filter_name = f"Filter_{emission_wavelength}"
                emission_filters.append(DeviceConfig(device_name=filter_name))
            
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
                )
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
            refractive_index=float(chamber_ri) if chamber_ri else 1.0
        )
        
        # Parse sample immersion data if available
        sample_immersion = None
        if metadata.slims_metadata.sample_immersion_medium:
            sample_medium = metadata.slims_metadata.sample_immersion_medium
            sample_ri = metadata.slims_metadata.sample_refractive_index
            sample_immersion = Immersion(
                medium=self._map_immersion_medium(sample_medium),
                refractive_index=float(sample_ri) if sample_ri else 1.0
            )
        
        return SampleChamberConfig(
            device_name="Sample Chamber",
            chamber_immersion=chamber_immersion,
            sample_immersion=sample_immersion
        )
    
    def _get_active_devices(self, metadata: SmartspimModel) -> List[str]:
        """Get list of active device names."""
        devices = []
        
        # Add instrument device
        if metadata.slims_metadata.instrument_id:
            devices.append(metadata.slims_metadata.instrument_id)
        
        # Add channel-specific devices
        imaging_channels = metadata.slims_metadata.imaging_channels or []
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
            return None, None
            
        channel_config = wavelength_config[channel_name]
        if isinstance(channel_config, dict):
            # Look for power settings (left/right or general)
            power = (
                channel_config.get("power") or
                channel_config.get("power_left") or
                channel_config.get("power_right")
            )
            if power is not None:
                return float(power), PowerUnit.PERCENT
        
        return None, None
    
    def _build_images(self, metadata: SmartspimModel) -> List[ImageSPIM]:
        """Build ImageSPIM objects for spatial/tile information."""
        images = []
        
        # Get imaging channels and create images for each
        imaging_channels = metadata.slims_metadata.imaging_channels or []
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
                Translation(translation=[0.0, 0.0, 0.0])
            ]
            
            image = ImageSPIM(
                channel_name=channel_display_name,
                file_name=AssetPath(filename),
                imaging_angle=0,  # Default angle
                imaging_angle_unit=AngleUnit.DEG,
                image_start_time=metadata.file_metadata.session_start_time,
                image_end_time=metadata.file_metadata.session_end_time,
                image_to_acquisition_transform=image_transform,
                dimensions_unit=SizeUnit.PX
            )
            
            images.append(image)
        
        return images
    
    def _map_immersion_medium(self, medium: Optional[str]) -> ImmersionMedium:
        """Map SLIMS immersion medium to schema enum."""
        if not medium:
            return ImmersionMedium.AIR
            
        medium_lower = medium.lower().strip()
        
        # Map based on the old implementation patterns
        if medium_lower in ["dih2o", "water"]:
            return ImmersionMedium.WATER
        elif "cargille oil" in medium_lower:
            return ImmersionMedium.OIL
        elif "ethyl cinnamate" in medium_lower:
            return ImmersionMedium.ECI
        elif "easyindex" in medium_lower:
            return ImmersionMedium.EASYINDEX
        else:
            return ImmersionMedium.OTHER