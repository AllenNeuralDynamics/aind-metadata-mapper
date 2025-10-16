"""FIP mapper module.

This mapper transforms intermediate FIP metadata into schema-compliant Acquisition objects.

TODO - Enhancements needed for full schema compliance:
======================================================

1. CHANNEL METADATA (requires rig/instrument endpoint):
   - intended_measurement: Need to map ROIs to measurements ("dopamine", "calcium", "control", etc.)
   - light_sources: Need to link specific LEDs to specific channels/ROIs
   - excitation_filters: Requires filter specifications from rig config
   - emission_filters: Requires filter specifications from rig config
   - emission_wavelength: Can be inferred from LED wavelengths but needs validation

2. CONNECTION GRAPH (requires instrument metadata endpoint):
   - Need full signal flow: LED → Patch Cord → Implanted Fiber → Patch Cord → Detector
   - Requires implanted fiber identifiers (not just camera ROIs)
   - Need to model bidirectional connections (send_and_receive=True)
   - Port mappings between devices

3. DEVICE DETAILS:
   - Power measurements at specific points (e.g., "patch cord end")
   - Actual exposure times from camera metadata (currently placeholder 10ms)
   - Serial numbers and calibration data for all devices

4. FIBER IMPLANT INFO:
   - Implanted fiber identifiers are now included (Fiber 0, Fiber 1, etc.)
   - ROI index N → Patch Cord N → Fiber N (implanted fiber)
   - Connection graph still needs to be added to DataStream.connections

CURRENT MAPPING:
================
- ROI index in camera matches Patch Cord index matches Fiber implant index
- Example: camera_green_iso_roi[0] → Patch Cord 0 → Fiber 0
- Channel names: "Fiber_N_<camera>" where N is the ROI/patch cord/fiber index

Current implementation provides a minimal valid schema that satisfies basic requirements.
Expert review and enhancement with rig/instrument metadata is recommended.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional
from zoneinfo import ZoneInfo

from aind_data_schema.core.acquisition import Acquisition, AcquisitionSubjectDetails, DataStream
from aind_data_schema.components.configs import (
    DetectorConfig,
    LightEmittingDiodeConfig,
    TriggerType,
    PatchCordConfig,
    Channel,
    DeviceConfig,
)
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import MassUnit, PowerUnit, SizeUnit, TimeUnit
from aind_metadata_extractor.models.fip import FIPDataModel


class FIPMapper:
    """FIP Mapper - transforms intermediate FIP data into Acquisition metadata."""

    def transform(self, metadata: dict) -> Acquisition:
        """Transforms intermediate metadata into a complete Acquisition model.

        Parameters
        ----------
        metadata : dict
            Metadata extracted from FIP files via the extractor.

        Returns
        -------
        Acquisition
            Fully composed acquisition model.
        """
        fip_metadata = FIPDataModel.model_validate(metadata)
        return self._transform(fip_metadata)

    def _transform(self, metadata: FIPDataModel) -> Acquisition:
        """Internal transform method.

        Parameters
        ----------
        metadata : FIPDataModel
            Validated intermediate metadata model.

        Returns
        -------
        Acquisition
            Complete acquisition metadata.
        """
        subject_id = metadata.subject_id
        instrument_id = metadata.rig_id
        
        protocol_id = None
        if metadata.iacuc_protocol:
            protocol_id = [metadata.iacuc_protocol]

        session_start_time, session_end_time = self._process_session_times(
            metadata.session_start_time,
            metadata.session_end_time,
        )

        subject_details = self._build_subject_details(metadata)

        data_stream = DataStream(
            stream_start_time=session_start_time,
            stream_end_time=session_end_time,
            modalities=[Modality.FIB],
            active_devices=self._get_active_devices(metadata),
            configurations=self._build_configurations(metadata),
        )

        acquisition = Acquisition(
            subject_id=subject_id,
            acquisition_start_time=session_start_time,
            acquisition_end_time=session_end_time,
            experimenters=metadata.experimenter_full_name,
            protocol_id=protocol_id,
            instrument_id=instrument_id,
            acquisition_type=metadata.session_type,
            notes=metadata.notes,
            data_streams=[data_stream],
            stimulus_epochs=[],
            subject_details=subject_details,
        )

        return acquisition

    def _build_subject_details(self, metadata: FIPDataModel) -> Optional[AcquisitionSubjectDetails]:
        """Build subject details from metadata.

        Parameters
        ----------
        metadata : FIPDataModel
            Validated intermediate metadata.

        Returns
        -------
        Optional[AcquisitionSubjectDetails]
            Subject details if any relevant fields are present.
        """
        if not metadata.mouse_platform_name:
            return None

        return AcquisitionSubjectDetails(
            mouse_platform_name=metadata.mouse_platform_name,
            animal_weight_prior=metadata.animal_weight_prior,
            animal_weight_post=metadata.animal_weight_post,
            weight_unit=MassUnit.G,
            anaesthesia=metadata.anaesthesia,
        )

    def _build_configurations(self, metadata: FIPDataModel) -> List[Any]:
        """Build device configurations from rig config.

        Parameters
        ----------
        metadata : FIPDataModel
            Validated intermediate metadata.

        Returns
        -------
        List[Any]
            List of device configurations (LEDs and detectors).
        """
        configurations = []
        rig_config = metadata.rig_config

        light_source_names = [
            name for name in rig_config.keys()
            if name.startswith('light_source_')
        ]
        
        for light_source_name in light_source_names:
            light_source = rig_config[light_source_name]
            led_name = light_source_name.replace('light_source_', '').upper()
            
            led_config = LightEmittingDiodeConfig(
                device_name=f"LED_{led_name}",
                power=light_source.get("power", 1.0),
                power_unit=PowerUnit.PERCENT,
            )
            configurations.append(led_config)

        camera_names = [
            name for name in rig_config.keys()
            if name.startswith('camera_')
        ]
        
        for camera_name in camera_names:
            camera = rig_config[camera_name]
            detector_name = camera_name.replace('camera_', '').replace('_', ' ').title()
            
            detector_config = DetectorConfig(
                device_name=f"Camera_{detector_name}",
                exposure_time=10.0,
                exposure_time_unit=TimeUnit.MS,
                trigger_type=TriggerType.INTERNAL,
            )
            configurations.append(detector_config)

        # Build patch cord configurations
        # Each ROI index corresponds to: Patch Cord N → Fiber N (implant)
        # ROI 0 → Patch Cord 0 → Fiber 0, etc.
        roi_settings = rig_config.get('roi_settings', {})
        if roi_settings:
            # Collect all ROIs across cameras and create patch cords
            roi_list = []
            
            for roi_key in roi_settings.keys():
                if '_roi' in roi_key and '_background' not in roi_key:
                    roi_data = roi_settings[roi_key]
                    camera_name = roi_key.replace('_roi', '')
                    
                    if isinstance(roi_data, list):
                        for idx, roi in enumerate(roi_data):
                            roi_list.append({
                                'roi_idx': idx,
                                'camera_name': camera_name,
                                'roi': roi
                            })
            
            # Create one patch cord per ROI index
            # Group by ROI index to handle multiple cameras
            roi_by_index = {}
            for roi_info in roi_list:
                roi_idx = roi_info['roi_idx']
                if roi_idx not in roi_by_index:
                    roi_by_index[roi_idx] = []
                roi_by_index[roi_idx].append(roi_info)
            
            # Create patch cord for each ROI index
            for roi_idx in sorted(roi_by_index.keys()):
                channels = []
                
                for roi_info in roi_by_index[roi_idx]:
                    camera_name = roi_info['camera_name']
                    emission_wl = self._infer_emission_wavelength(camera_name)
                    
                    # Channel name reflects camera and fiber index
                    channel = Channel(
                        channel_name=f"Fiber_{roi_idx}_{camera_name.replace('camera_', '')}",
                        intended_measurement=None,  # TODO: Requires ROI-to-measurement mapping
                        detector=DetectorConfig(
                            device_name=f"Camera_{camera_name.replace('camera_', '').replace('_', ' ').title()}",
                            exposure_time=10.0,  # TODO: Extract from camera metadata
                            exposure_time_unit=TimeUnit.MS,
                            trigger_type=TriggerType.INTERNAL,
                        ),
                        light_sources=[],  # TODO: Need LED-to-ROI mapping from rig config
                        excitation_filters=[],  # TODO: Requires filter specs from instrument
                        emission_filters=[],  # TODO: Requires filter specs from instrument
                        emission_wavelength=emission_wl,
                        emission_wavelength_unit=SizeUnit.NM if emission_wl else None,
                    )
                    channels.append(channel)
                
                # Patch Cord N connects to Fiber N (implanted fiber)
                patch_cord = PatchCordConfig(
                    device_name=f"Patch Cord {roi_idx}",
                    channels=channels,
                )
                configurations.append(patch_cord)

        return configurations
    
    def _infer_emission_wavelength(self, camera_name: str) -> Optional[int]:
        """Infer emission wavelength from camera name.
        
        This is a rough estimate based on common FIP setups.
        TODO: Replace with actual wavelength data from rig config.
        
        Parameters
        ----------
        camera_name : str
            Camera name from rig config.
        
        Returns
        -------
        Optional[int]
            Estimated emission wavelength in nm, or None if unknown.
        """
        camera_lower = camera_name.lower()
        if 'green' in camera_lower:
            return 510  # Typical green emission
        elif 'red' in camera_lower:
            return 590  # Typical red emission
        return None

    def _get_active_devices(self, metadata: FIPDataModel) -> List[str]:
        """Get list of active device names.
        
        Includes implanted fibers based on ROI count.
        Each ROI index corresponds to: Patch Cord N → Fiber N (implant).

        Parameters
        ----------
        metadata : FIPDataModel
            Validated intermediate metadata.

        Returns
        -------
        List[str]
            List of active device names.
        """
        devices = []
        
        if metadata.rig_id:
            devices.append(metadata.rig_id)

        rig_config = metadata.rig_config

        # Add LEDs
        light_source_names = [
            name for name in rig_config.keys()
            if name.startswith('light_source_')
        ]
        for light_source_name in light_source_names:
            led_name = light_source_name.replace('light_source_', '').upper()
            devices.append(f"LED_{led_name}")

        # Add cameras
        camera_names = [
            name for name in rig_config.keys()
            if name.startswith('camera_')
        ]
        for camera_name in camera_names:
            detector_name = camera_name.replace('camera_', '').replace('_', ' ').title()
            devices.append(f"Camera_{detector_name}")

        # Add patch cords and implanted fibers (zero-indexed)
        # Patch Cord N connects to Fiber N
        roi_settings = rig_config.get('roi_settings', {})
        if roi_settings:
            # Find max ROI index across all cameras
            max_roi_idx = -1
            for roi_key in roi_settings.keys():
                if '_roi' in roi_key and '_background' not in roi_key:
                    roi_data = roi_settings[roi_key]
                    if isinstance(roi_data, list):
                        max_roi_idx = max(max_roi_idx, len(roi_data) - 1)
            
            # Add patch cords and fibers for each ROI index
            for roi_idx in range(max_roi_idx + 1):
                devices.append(f"Patch Cord {roi_idx}")
                devices.append(f"Fiber {roi_idx}")

        # Add controller
        if 'cuttlefish_fip' in rig_config:
            devices.append("cuTTLefishFip")

        return devices

    def _process_session_times(self, session_start_time, session_end_time):
        """Process and validate session times.

        Parameters
        ----------
        session_start_time : datetime or str
            Session start time.
        session_end_time : datetime or str
            Session end time.

        Returns
        -------
        tuple[datetime, datetime]
            Processed start and end times.
        """
        def ensure_timezone(dt):
            """Ensure datetime has timezone info."""
            if dt is None:
                return datetime.now(ZoneInfo("America/Los_Angeles"))
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("America/Los_Angeles"))
            return dt

        session_start_time = ensure_timezone(session_start_time)
        session_end_time = ensure_timezone(session_end_time)

        if session_start_time > session_end_time:
            session_start_time, session_end_time = session_end_time, session_start_time

        return session_start_time, session_end_time

    def write(self, model: Acquisition, filename: str = "acquisition.json", output_directory: Optional[str] = None):
        """Write the Acquisition model to a JSON file.

        Parameters
        ----------
        model : Acquisition
            The acquisition model to write.
        filename : str, optional
            Output filename, by default "acquisition.json".
        output_directory : Optional[str], optional
            Output directory path, by default None (current directory).
        """
        if output_directory:
            output_path = Path(output_directory) / filename
        else:
            output_path = Path(filename)
        
        with open(output_path, 'w') as f:
            f.write(model.model_dump_json(indent=2))
        
        print(f"Wrote acquisition metadata to {output_path}")

