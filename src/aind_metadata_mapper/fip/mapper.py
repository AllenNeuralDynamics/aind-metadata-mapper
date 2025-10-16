"""FIP mapper module.

This mapper transforms intermediate FIP metadata into schema-compliant Acquisition objects.

FIBER PHOTOMETRY SYSTEM ARCHITECTURE:
======================================
Each implanted fiber has 3 temporal-multiplexed channels (60Hz cycling):
  1. Green Channel: 470nm (blue LED) excitation → ~510nm emission → Green CMOS
  2. Isosbestic Channel: 415nm (UV LED) excitation → 490-540nm emission → Green CMOS (same camera!)
  3. Red Channel: 565nm (yellow LED) excitation → ~590nm emission → Red CMOS

CURRENT IMPLEMENTATION:
=======================
- Creates 3 channels per fiber (green, isosbestic, red)
- Green and isosbestic channels share same detector (green CMOS) but have different excitation
- Fetches intended_measurement from metadata service endpoint
- Fetches implanted fiber indices from procedures endpoint (validates which fibers exist)
- Only creates patch cord configurations for actually implanted fibers
- LED wavelengths included in device names (LED_UV_415nm, LED_BLUE_470nm, LED_LIME_565nm)
- LED references added to each channel's light_sources field
- Emission wavelengths: 520nm green/iso, 590nm red
- ROI index N → Patch Cord N → Fiber N (zero-indexed correspondence)
- Implanted fiber identifiers included in active_devices (Fiber 0, Fiber 1, etc.)

TODO - Enhancements for full schema compliance:
================================================

1. FILTER SPECIFICATIONS (requires instrument endpoint):
   - Excitation filters: Need specs for 415nm, 470nm, 565nm paths
   - Emission filters: Need dichroic and bandpass filter specifications

2. CONNECTION GRAPH (requires instrument metadata endpoint):
   - Full signal path: LED → Fiber Coupler → Patch Cord → Implanted Fiber → Patch Cord → Dichroic → Filter → Camera
   - Model temporal multiplexing (LED cycling, camera synchronization)
   - Bidirectional fiber connections
   - Port/channel mappings between devices
   - Add to DataStream.connections field

3. DETAILED DEVICE METADATA (requires camera/instrument metadata):
   - LED power calibration at patch cord end
   - Camera exposure times (currently using PLACEHOLDER_CAMERA_EXPOSURE_TIME = -1)
   - Camera serial numbers, gain settings, ROI coordinates
   - Temporal multiplexing timing (16.67ms period, LED pulse widths)
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
)
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import MassUnit, PowerUnit, SizeUnit, TimeUnit
from aind_metadata_extractor.models.fip import FIPDataModel


# NOTE: Wavelength values are currently hardcoded as constants.
# Ideally, these would be pulled from an instrument configuration file or some
# other source of truth. We should update this if we can identify a better source.

# FIP system LED excitation wavelengths (nm)
EXCITATION_UV = 415      # UV LED → green emission (isosbestic control)
EXCITATION_BLUE = 470    # Blue LED → green emission (GFP signal)
EXCITATION_YELLOW = 565  # Yellow/Lime LED → red emission (RFP signal)

# FIP system emission wavelengths (nm)
EMISSION_GREEN = 520  # Green emission: center of 490-540nm bandpass, ~510nm GFP peak
EMISSION_RED = 590    # Red emission: ~590nm RFP peak

# Placeholder for camera exposure time (milliseconds)
# TODO: Replace with actual exposure time from camera metadata when available
# Using -1 as an obviously invalid value to indicate missing data
PLACEHOLDER_CAMERA_EXPOSURE_TIME = -1


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
    
    def get_implanted_fibers(self, subject_id: str) -> Optional[List[int]]:
        """Fetch implanted fiber indices from procedures endpoint.
        
        Queries internal metadata service procedures endpoint to determine which
        fibers were actually implanted during surgery. This prevents creating
        patch cord connections to non-existent implanted fibers.
        
        Note: This endpoint can be slow (~30 seconds).
        
        Parameters
        ----------
        subject_id : str
            Subject ID to query.
        
        Returns
        -------
        Optional[List[int]]
            List of implanted fiber indices (e.g., [0, 1, 2] for Fiber_0, Fiber_1, Fiber_2).
            Returns None if request fails or no fiber probes found.
        """
        try:
            url = f"http://aind-metadata-service-dev/api/v2/procedures/{subject_id}"
            response = requests.get(url, timeout=60)  # Longer timeout for slow endpoint
            response.raise_for_status()
            
            data = response.json()
            implanted_indices = set()
            
            # Look through subject_procedures for fiber probe implants
            for subject_proc in data.get('subject_procedures', []):
                if subject_proc.get('object_type') != 'Surgery':
                    continue
                    
                for proc in subject_proc.get('procedures', []):
                    if proc.get('object_type') != 'Probe implant':
                        continue
                    
                    # Check if this is a fiber probe implant
                    implanted_device = proc.get('implanted_device', {})
                    if implanted_device.get('object_type') == 'Fiber probe':
                        # Extract fiber index from name (e.g., "Fiber_0" -> 0)
                        fiber_name = implanted_device.get('name', '')
                        if fiber_name.startswith('Fiber_'):
                            try:
                                fiber_idx = int(fiber_name.split('_')[1])
                                implanted_indices.add(fiber_idx)
                            except (IndexError, ValueError):
                                # Skip if we can't parse the fiber index
                                continue
            
            if implanted_indices:
                return sorted(list(implanted_indices))
            return None
            
        except Exception as e:
            # Log but don't fail - we'll fall back to ROI-based inference
            print(f"Warning: Could not fetch implanted fibers from procedures: {e}")
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
        
        ethics_review_id = None
        if metadata.ethics_review_id:
            ethics_review_id = [metadata.ethics_review_id]

        session_start_time, session_end_time = self._process_session_times(
            metadata.session_start_time,
            metadata.session_end_time,
        )

        subject_details = self._build_subject_details(metadata)
        
        # Fetch intended measurements and implanted fibers from metadata service
        intended_measurements = self.get_intended_measurements(subject_id)
        implanted_fibers = self.get_implanted_fibers(subject_id)

        data_stream = DataStream(
            stream_start_time=session_start_time,
            stream_end_time=session_end_time,
            modalities=[Modality.FIB],
            active_devices=self._get_active_devices(metadata, implanted_fibers),
            configurations=self._build_configurations(metadata, intended_measurements, implanted_fibers),
        )

        acquisition = Acquisition(
            subject_id=subject_id,
            acquisition_start_time=session_start_time,
            acquisition_end_time=session_end_time,
            experimenters=metadata.experimenter_full_name,
            ethics_review_id=ethics_review_id,
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

    def _build_configurations(
        self, 
        metadata: FIPDataModel,
        intended_measurements: Optional[Dict[str, Dict[str, Optional[str]]]] = None,
        implanted_fibers: Optional[List[int]] = None
    ) -> List[Any]:
        """Build device configurations from rig config.

        Parameters
        ----------
        metadata : FIPDataModel
            Validated intermediate metadata.
        intended_measurements : Optional[Dict[str, Dict[str, Optional[str]]]], optional
            Intended measurements from metadata service, mapping fiber names to
            channel measurements (R, G, B, Iso), by default None.
        implanted_fibers : Optional[List[int]], optional
            List of implanted fiber indices from procedures endpoint. Only creates
            patch cord configurations for implanted fibers. Falls back to ROI-based
            inference if None, by default None.

        Returns
        -------
        List[Any]
            List of device configurations (LEDs, detectors, and patch cords).
        """
        configurations = []
        rig_config = metadata.rig_config

        # Build LED configs and store them for later reference
        led_configs_by_wavelength = {}  # Map wavelength to LED config
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

            # Store for reference in channels
            if wavelength:
                led_configs_by_wavelength[wavelength] = led_config

        # Note: Camera configurations are created inline within Channel objects below
        # We don't create standalone DetectorConfig objects here because we don't
        # have accurate exposure times or other camera settings from the rig_config

        # Build patch cord configurations
        # Each ROI index corresponds to: Patch Cord N → Fiber N (implant)
        # ROI 0 → Patch Cord 0 → Fiber 0, etc.
        # 
        # Each fiber has 3 channels due to temporal multiplexing:
        #   1. Green: 470nm excitation, ~520nm emission, green camera
        #   2. Isosbestic: 415nm excitation, ~520nm emission, green camera (same detector!)
        #   3. Red: 565nm excitation, ~590nm emission, red camera
        roi_settings = rig_config.get('roi_settings', {})
        if roi_settings:
            # Find which cameras have ROIs defined
            has_green_camera = any('green' in key or 'iso' in key for key in roi_settings.keys() if '_roi' in key and '_background' not in key)
            has_red_camera = any('red' in key for key in roi_settings.keys() if '_roi' in key and '_background' not in key)
            
            # Determine which fibers to create patch cords for
            # Priority: implanted_fibers (from procedures) > ROI counts (fallback)
            if implanted_fibers is not None:
                fiber_indices = implanted_fibers
            else:
                # Fallback: infer from ROI counts
                max_fiber_count = 0
                for roi_key in roi_settings.keys():
                    if '_roi' in roi_key and '_background' not in roi_key:
                        roi_data = roi_settings[roi_key]
                        if isinstance(roi_data, list):
                            max_fiber_count = max(max_fiber_count, len(roi_data))
                fiber_indices = list(range(max_fiber_count))
            
            # Create patch cord for each implanted fiber
            for fiber_idx in fiber_indices:
                channels = []
                fiber_name = f"Fiber_{fiber_idx}"
                fiber_measurements = intended_measurements.get(fiber_name) if intended_measurements else None
                
                # Create Green channel: Blue LED (470nm) → Green emission (520nm)
                if has_green_camera:
                    green_measurement = fiber_measurements.get('G') if fiber_measurements else None
                    blue_led = led_configs_by_wavelength.get(EXCITATION_BLUE)
                    channels.append(Channel(
                        channel_name=f"Fiber_{fiber_idx}_Green",
                        intended_measurement=green_measurement,
                        detector=DetectorConfig(
                            device_name="Camera_Green Iso",
                            exposure_time=PLACEHOLDER_CAMERA_EXPOSURE_TIME,
                            exposure_time_unit=TimeUnit.MS,
                            trigger_type=TriggerType.INTERNAL,
                        ),
                        light_sources=[blue_led] if blue_led else [],
                        excitation_filters=[],  # TODO: Requires filter specs from instrument
                        emission_filters=[],  # TODO: Requires filter specs from instrument
                        emission_wavelength=EMISSION_GREEN,
                        emission_wavelength_unit=SizeUnit.NM,
                    ))
                
                # Create Isosbestic channel: UV LED (415nm) → Green emission (520nm)
                if has_green_camera:
                    iso_measurement = fiber_measurements.get('Iso') if fiber_measurements else None
                    uv_led = led_configs_by_wavelength.get(EXCITATION_UV)
                    channels.append(Channel(
                        channel_name=f"Fiber_{fiber_idx}_Isosbestic",
                        intended_measurement=iso_measurement,
                        detector=DetectorConfig(
                            device_name="Camera_Green Iso",
                            exposure_time=PLACEHOLDER_CAMERA_EXPOSURE_TIME,
                            exposure_time_unit=TimeUnit.MS,
                            trigger_type=TriggerType.INTERNAL,
                        ),
                        light_sources=[uv_led] if uv_led else [],
                        excitation_filters=[],  # TODO: Requires filter specs from instrument
                        emission_filters=[],  # TODO: Requires filter specs from instrument
                        emission_wavelength=EMISSION_GREEN,
                        emission_wavelength_unit=SizeUnit.NM,
                    ))
                
                # Create Red channel: Yellow LED (565nm) → Red emission (590nm)
                if has_red_camera:
                    red_measurement = fiber_measurements.get('R') if fiber_measurements else None
                    yellow_led = led_configs_by_wavelength.get(EXCITATION_YELLOW)
                    channels.append(Channel(
                        channel_name=f"Fiber_{fiber_idx}_Red",
                        intended_measurement=red_measurement,
                        detector=DetectorConfig(
                            device_name="Camera_Red",
                            exposure_time=PLACEHOLDER_CAMERA_EXPOSURE_TIME,
                            exposure_time_unit=TimeUnit.MS,
                            trigger_type=TriggerType.INTERNAL,
                        ),
                        light_sources=[yellow_led] if yellow_led else [],
                        excitation_filters=[],  # TODO: Requires filter specs from instrument
                        emission_filters=[],  # TODO: Requires filter specs from instrument
                        emission_wavelength=EMISSION_RED,
                        emission_wavelength_unit=SizeUnit.NM,
                    ))
                
                # Patch Cord N connects to Fiber N (implanted fiber)
                if channels:  # Only create patch cord if we have channels
                    patch_cord = PatchCordConfig(
                        device_name=f"Patch Cord {fiber_idx}",
                        channels=channels,
                    )
                    configurations.append(patch_cord)

        return configurations

    def _get_led_wavelength(self, led_name: str) -> Optional[int]:
        """Get LED excitation wavelength based on LED name.
        
        FIP system uses 3 LEDs for excitation, each producing different emission:
        - UV LED (415nm excitation) → green emission (isosbestic control)
        - Blue LED (470nm excitation) → green emission (GFP signal)
        - Yellow/Lime LED (565nm excitation) → red emission (RFP signal)
        
        Parameters
        ----------
        led_name : str
            LED name (e.g., "UV", "BLUE", "YELLOW", "LIME").
        
        Returns
        -------
        Optional[int]
            LED excitation wavelength in nm, or None if unknown.
        """
        led_lower = led_name.lower()
        wavelength_map = {
            'uv': EXCITATION_UV,
            '415': EXCITATION_UV,
            'blue': EXCITATION_BLUE,
            '470': EXCITATION_BLUE,
            'yellow': EXCITATION_YELLOW,
            'lime': EXCITATION_YELLOW,
            '565': EXCITATION_YELLOW,
            '560': EXCITATION_YELLOW,
        }
        for key, wavelength in wavelength_map.items():
            if key in led_lower:
                return wavelength
        return None

    def _get_active_devices(self, metadata: FIPDataModel, implanted_fibers: Optional[List[int]] = None) -> List[str]:
        """Get list of active device names.
        
        Includes implanted fibers and patch cords based on procedures data or ROI count.
        Each ROI index corresponds to: Patch Cord N → Fiber N (implant).

        Parameters
        ----------
        metadata : FIPDataModel
            Validated intermediate metadata.
        implanted_fibers : Optional[List[int]], optional
            List of implanted fiber indices from procedures endpoint. Falls back to
            ROI-based inference if None, by default None.

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
        # Priority: implanted_fibers (from procedures) > ROI counts (fallback)
        if implanted_fibers is not None:
            fiber_indices = implanted_fibers
        else:
            # Fallback: infer from ROI counts
            roi_settings = rig_config.get('roi_settings', {})
            fiber_indices = []
            if roi_settings:
                # Find max ROI index across all cameras
                max_roi_idx = -1
                for roi_key in roi_settings.keys():
                    if '_roi' in roi_key and '_background' not in roi_key:
                        roi_data = roi_settings[roi_key]
                        if isinstance(roi_data, list):
                            max_roi_idx = max(max_roi_idx, len(roi_data) - 1)
                fiber_indices = list(range(max_roi_idx + 1))
        
        # Add patch cords and fibers for each implanted fiber
        for fiber_idx in fiber_indices:
            devices.append(f"Patch Cord {fiber_idx}")
            devices.append(f"Fiber {fiber_idx}")

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

