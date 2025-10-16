"""FIP mapper module.

This mapper transforms intermediate FIP metadata into schema-compliant Acquisition objects.

TODO - Enhancements needed for full schema compliance:
======================================================

FIBER PHOTOMETRY SYSTEM ARCHITECTURE:
Each implanted fiber has 3 temporal-multiplexed channels (60Hz cycling):
  1. Green Channel: 470nm (blue LED) excitation → ~510nm emission → Green CMOS
  2. Isosbestic Channel: 415nm (UV LED) excitation → 490-540nm emission → Green CMOS (same camera!)
  3. Red Channel: 565nm (yellow LED) excitation → ~590nm emission → Red CMOS

1. CHANNEL STRUCTURE (partially implemented):
   Current: Creating 1-2 channels per fiber (one per camera detecting that fiber)
   Needed: Create 3 channels per fiber (green, isosbestic, red)
   - Green CMOS captures TWO channels: green (470nm excitation) + isosbestic (415nm excitation)
   - Red CMOS captures ONE channel: red (565nm excitation)
   - intended_measurement: Need ROI-to-measurement mapping ("dopamine", "calcium", "isosbestic_control")
   - light_sources: Need to map correct LED to each channel (requires LED-to-ROI mapping from rig config)

2. WAVELENGTH & FILTER DATA (requires instrument endpoint):
   - Excitation filters: Need specifications for 415nm, 470nm, 565nm paths
   - Emission filters: Need dichroic and bandpass filter specs
   - emission_wavelength: Currently using placeholders (525nm, 600nm)
   - Accurate wavelengths: Green ~510nm peak, Isosbestic 490-540nm bandpass, Red ~590nm peak

3. CONNECTION GRAPH (requires instrument metadata endpoint):
   - Full signal path: LED → Fiber Coupler → Patch Cord → Implanted Fiber → Patch Cord → Dichroic → Filter → Camera
   - Need to model temporal multiplexing (LEDs cycle, cameras synchronize)
   - Bidirectional fiber connections (send excitation, receive emission)
   - Port/channel mappings between devices

4. DEVICE DETAILS:
   - LED power calibration at patch cord end
   - Actual camera exposure times (currently placeholder 10ms)
   - Camera serial numbers, gain settings, ROI coordinates
   - Temporal multiplexing timing parameters (16.67ms period, LED pulse widths)

5. FIBER IMPLANT INFO:
   - Implanted fiber identifiers now included (Fiber 0, Fiber 1, etc.)
   - ROI index N → Patch Cord N → Fiber N (zero-indexed correspondence)
   - Connection graph needs to be added to DataStream.connections

CURRENT MAPPING:
================
- ROI index in camera matches Patch Cord index matches Fiber implant index
- Example: camera_green_iso_roi[0] → Patch Cord 0 → Fiber 0
- Channel names: "Fiber_N_<camera>" where N is the ROI/patch cord/fiber index
- Each patch cord currently has 1-2 channels; should have 3 (green + isosbestic + red)

Current implementation provides a minimal valid schema that satisfies basic requirements.
Full 3-channel-per-fiber implementation requires LED-to-ROI mapping from rig config.
Expert review and enhancement with rig/instrument metadata is recommended.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

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
    """FIP Mapper - transforms intermediate FIP data into Acquisition metadata.
    
    This mapper follows the standard pattern for AIND metadata mappers:
    - Takes intermediate metadata from extractor
    - Transforms to schema-compliant Acquisition
    - Outputs to standard filename: acquisition.json (configurable)
    
    Parameters
    ----------
    output_filename : str, optional
        Output filename for the acquisition metadata.
        Defaults to "acquisition.json".
    """
    
    def __init__(self, output_filename: str = "acquisition.json"):
        """Initialize the FIP mapper.
        
        Parameters
        ----------
        output_filename : str, optional
            Output filename, by default "acquisition.json"
        """
        self.output_filename = output_filename

    def get_intended_measurements(self, subject_id: str) -> Optional[Dict[str, Dict[str, Optional[str]]]]:
        """Fetch intended measurements for a subject from the metadata service.
        
        Queries http://aind-metadata-service/intended_measurements/{subject_id}
        to get the measurement assignments for each fiber and color channel.
        
        Parameters
        ----------
        subject_id : str
            The subject ID to query.
            
        Returns
        -------
        Optional[Dict[str, Dict[str, Optional[str]]]]
            Dictionary mapping fiber names to channel measurements, e.g.:
            {
                "Fiber_0": {
                    "R": "dopamine",      # Red channel
                    "G": "calcium",       # Green channel  
                    "B": None,            # Blue channel (typically unused)
                    "Iso": "control"      # Isosbestic channel
                },
                "Fiber_1": {...}
            }
            Returns None if the request fails or subject has no measurements.
        """
        try:
            url = f"http://aind-metadata-service/intended_measurements/{subject_id}"
            response = requests.get(url, timeout=5)
            
            if response.status_code not in [200, 300]:
                print(f"Warning: Could not fetch intended measurements for subject {subject_id} (status {response.status_code})")
                return None
            
            data = response.json()
            
            # Handle both single object and array responses
            measurements_list = data.get('data', [])
            if isinstance(measurements_list, dict):
                measurements_list = [measurements_list]
            
            # Convert to fiber-indexed dictionary
            result = {}
            for item in measurements_list:
                fiber_name = item.get('fiber_name')
                if fiber_name:
                    result[fiber_name] = {
                        'R': item.get('intended_measurement_R'),
                        'G': item.get('intended_measurement_G'),
                        'B': item.get('intended_measurement_B'),
                        'Iso': item.get('intended_measurement_Iso'),
                    }
            
            return result if result else None
            
        except Exception as e:
            print(f"Warning: Error fetching intended measurements for subject {subject_id}: {e}")
            return None

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
            wavelength = self._get_led_wavelength(led_name)
            
            # Include wavelength in device name for clarity
            # LightEmittingDiodeConfig doesn't have excitation_wavelength fields yet
            device_name = f"LED_{led_name}"
            if wavelength:
                device_name = f"LED_{led_name}_{wavelength}nm"
            
            led_config = LightEmittingDiodeConfig(
                device_name=device_name,
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
        # 
        # IMPORTANT: Each fiber should have 3 channels (green, isosbestic, red) due to
        # temporal multiplexing, but we currently create 1-2 channels based on which
        # cameras have ROIs defined. Full 3-channel implementation requires:
        # - LED-to-ROI mapping to assign correct light sources
        # - ROI-to-measurement mapping for intended_measurement field
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
                
                # Currently creating one channel per camera that sees this fiber
                # TODO: Should create 3 channels per fiber:
                #   1. Green: 470nm excitation, ~520nm emission, green camera
                #   2. Isosbestic: 415nm excitation, ~520nm emission, green camera
                #   3. Red: 565nm excitation, ~590nm emission, red camera
                for roi_info in roi_by_index[roi_idx]:
                    camera_name = roi_info['camera_name']
                    emission_wl = self._infer_emission_wavelength(camera_name)
                    
                    # Channel name reflects camera and fiber index
                    # Green camera captures 2 channels (green + isosbestic) but we only
                    # create one channel object here due to missing LED-to-ROI mapping
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
    
    def _get_led_wavelength(self, led_name: str) -> Optional[int]:
        """Get excitation wavelength for an LED based on its name.
        
        Based on standard FIP system configuration:
        - UV/415: 415nm excitation → isosbestic channel
        - Blue/470: 470nm excitation → green channel
        - Yellow/Lime/565: 565nm excitation → red channel
        
        Parameters
        ----------
        led_name : str
            LED name (e.g., "UV", "BLUE", "YELLOW", "LIME").
        
        Returns
        -------
        Optional[int]
            Excitation wavelength in nm, or None if unknown.
        """
        led_lower = led_name.lower()
        wavelength_map = {
            'uv': 415,
            '415': 415,
            'blue': 470,
            '470': 470,
            'yellow': 565,
            'lime': 565,
            '565': 565,
            '560': 565,
        }
        for key, wavelength in wavelength_map.items():
            if key in led_lower:
                return wavelength
        return None

    def _infer_emission_wavelength(self, camera_name: str) -> Optional[int]:
        """Infer emission wavelength from camera name.
        
        Based on FIP system architecture:
        - Green camera: 510nm peak (GFP) and 490-540nm (isosbestic, use 520nm center)
        - Red camera: 590nm peak (RFP)
        
        Note: Green camera captures both green and isosbestic channels with
        different excitation wavelengths but similar emission wavelengths.
        
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
        if 'green' in camera_lower or 'iso' in camera_lower:
            return 520  # Center of 490-540nm isosbestic bandpass / ~510nm GFP peak
        elif 'red' in camera_lower:
            return 590  # Red emission peak
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

    def run_job(self, metadata: dict, output_directory: Optional[str] = None) -> Path:
        """Run the complete mapping job: transform and write.
        
        This is the main entry point following the standard AIND mapper pattern.

        Parameters
        ----------
        metadata : dict
            Intermediate metadata from extractor.
        output_directory : Optional[str], optional
            Output directory path, by default None (current directory).

        Returns
        -------
        Path
            Path to the written acquisition file.
        """
        acquisition = self.transform(metadata)
        output_path = self.write(acquisition, output_directory)
        return output_path

    def write(self, model: Acquisition, output_directory: Optional[str] = None) -> Path:
        """Write the Acquisition model to a JSON file.
        
        The output filename is determined by the mapper's output_filename attribute
        (set during initialization, defaults to acquisition.json).

        Parameters
        ----------
        model : Acquisition
            The acquisition model to write.
        output_directory : Optional[str], optional
            Output directory path, by default None (current directory).

        Returns
        -------
        Path
            Path to the written file.
        """
        if output_directory:
            output_path = Path(output_directory) / self.output_filename
        else:
            output_path = Path(self.output_filename)
        
        with open(output_path, 'w') as f:
            f.write(model.model_dump_json(indent=2))
        
        print(f"Wrote acquisition metadata to {output_path}")
        return output_path

