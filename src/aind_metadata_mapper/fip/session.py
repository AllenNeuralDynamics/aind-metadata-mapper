"""Module for creating Fiber Photometry session metadata.

This module implements an ETL (Extract, Transform, Load) pattern for generating
standardized session metadata from fiber photometry experiments. It handles:

- Extraction of session times from data files
- Transformation of raw data into standardized session objects
- Loading/saving of session metadata in a standard format

The ETL class provides hooks for future extension to fetch additional data from
external services or handle new data formats.
"""

import sys
import json
from typing import Union, Optional, List
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import pandas as pd

from aind_data_schema.base import AindCoreModel
from aind_data_schema.core.session import (
    DetectorConfig,
    FiberConnectionConfig,
    LightEmittingDiodeConfig,
    Session,
    Stream,
    Channel,
    PowerUnit
)
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.fip.models import (
    JobSettings,
    DEFAULT_CHANNEL_PROPS,
    LED_WAVELENGTH_NAMES,
    FILTER_MAPPINGS,
    EXCITATION_FILTER_MAPPINGS,
    DETECTOR_MAPPINGS,
    CHANNEL_COLOR_MAP,
    EMISSION_TO_EXCITATION_MAP,
    EMISSION_WAVELENGTHS,
    CHANNEL_DESCRIPTIONS,
)
from aind_metadata_mapper.fip.utils import (
    extract_session_start_time_from_files,
    extract_session_end_time_from_files,
    check_fiber_indexing,
    normalize_fiber_name,
    extract_fiber_index
)
import requests


@dataclass
class FiberData:
    """Intermediate data model for fiber photometry data."""

    start_time: datetime
    end_time: Optional[datetime]
    data_files: List[Path]
    timestamps: List[float]
    light_source_configs: List[dict]
    detector_configs: List[dict]
    fiber_configs: List[dict]
    subject_id: str
    experimenter_full_name: List[str]
    rig_id: str
    iacuc_protocol: str
    notes: str
    mouse_platform_name: str
    active_mouse_platform: bool


class FIBEtl(GenericEtl[JobSettings]):
    """Creates fiber photometry session metadata using an ETL pattern.

    This class handles the full lifecycle of session metadata creation:
    - Extracting timing information from data files
    - Transforming raw data into standardized session objects
    - Loading/saving session metadata in a standard format

    The ETL process ensures that all required metadata fields are populated
    and validates the output against the AIND data schema.

    This class inherits from GenericEtl which provides the _load method
    for writing session metadata to a JSON file using a standard filename
    format (session_fip.json).
    """

    def __init__(self, job_settings: Union[str, JobSettings]):
        """Initialize ETL with job settings.

        Parameters
        ----------
        job_settings : Union[str, JobSettings]
            Either a JobSettings object or a JSON string that can
            be parsed into one. The settings define all required parameters
            for the session metadata, including experimenter info, subject
            ID, data paths, etc.

        Raises
        ------
        ValidationError
            If the provided settings fail schema validation
        JSONDecodeError
            If job_settings is a string but not valid JSON
        """
        if isinstance(job_settings, str):
            job_settings = JobSettings(**json.loads(job_settings))
        super().__init__(job_settings)

    def _extract(self) -> FiberData:
        """Extract metadata and raw data from fiber photometry files.

        This method parses the raw data files to create an
        intermediate data model containing all necessary
        information for creating a Session object.

        Returns
        -------
        FiberData
            Intermediate data model containing parsed file data and metadata
        """
        settings = self.job_settings
        data_dir = Path(settings.data_directory)

        data_files = list(data_dir.glob("FIP_Data*.csv"))
        start_time = extract_session_start_time_from_files(data_dir)
        end_time = (
            extract_session_end_time_from_files(data_dir, start_time)
            if start_time
            else None
        )

        timestamps = []
        for file in data_files:
            df = pd.read_csv(file, header=None)
            timestamps.extend(df[0].tolist())

        stream_data = settings.data_streams[0]

        return FiberData(
            start_time=start_time,
            end_time=end_time,
            data_files=data_files,
            timestamps=timestamps,
            light_source_configs=stream_data["light_sources"],
            detector_configs=stream_data["detectors"],
            fiber_configs=stream_data["fiber_connections"],
            subject_id=settings.subject_id,
            experimenter_full_name=settings.experimenter_full_name,
            rig_id=settings.rig_id,
            iacuc_protocol=settings.iacuc_protocol,
            notes=settings.notes,
            mouse_platform_name=settings.mouse_platform_name,
            active_mouse_platform=settings.active_mouse_platform,
        )

    def _transform(self, fiber_data: FiberData) -> Session:
        """Transform extracted data into a valid Session object.

        Parameters
        ----------
        fiber_data : FiberData
            Intermediate data model containing parsed file data and metadata

        Returns
        -------
        Session
            A fully configured Session object that
            conforms to the AIND data schema
        """
        stream = Stream(
            stream_start_time=fiber_data.start_time,
            stream_end_time=fiber_data.end_time,
            light_sources=[
                LightEmittingDiodeConfig(**ls)
                for ls in fiber_data.light_source_configs
            ],
            stream_modalities=[Modality.FIB],
            detectors=[
                DetectorConfig(**d) for d in fiber_data.detector_configs
            ],
            fiber_connections=[
                FiberConnectionConfig(**fc) for fc in fiber_data.fiber_configs
            ],
        )

        session = Session(
            experimenter_full_name=fiber_data.experimenter_full_name,
            session_start_time=fiber_data.start_time,
            session_end_time=fiber_data.end_time,
            session_type="FIB",
            rig_id=fiber_data.rig_id,
            subject_id=fiber_data.subject_id,
            iacuc_protocol=fiber_data.iacuc_protocol,
            notes=fiber_data.notes,
            data_streams=[stream],
            mouse_platform_name=fiber_data.mouse_platform_name,
            active_mouse_platform=fiber_data.active_mouse_platform,
        )

        return session

    def _load(
        self, output_model: AindCoreModel, output_directory: Optional[Path]
    ) -> JobResponse:
        """Override parent _load to handle custom
        filenames and default directories.

        This implementation differs from the parent GenericEtl._load
        in that it:
        1. Uses the filename specified in job_settings rather
        than model's default
        2. Falls back to data_directory if no output_directory specified
        3. Maintains validation and error handling from parent class

        Parameters
        ----------
        output_model : AindCoreModel
            The final model that has been constructed and validated
        output_directory : Optional[Path]
            Directory where the file should be written. If None,
            defaults to job_settings.data_directory

        Returns
        -------
        JobResponse
            Object containing status code, message, and optional data.
            Status codes:
            - 200: Success
            - 500: File writing errors
        """
        # If no output directory specified, use the data directory
        # TODO: add response from service here
        if output_directory is None:
            output_directory = Path(self.job_settings.data_directory)

        output_path = output_directory / self.job_settings.output_filename
        with open(output_path, "w") as f:
            f.write(output_model.model_dump_json(indent=3))
        return JobResponse(
            status_code=200, message=f"Write model to {output_path}"
        )
    
    def _extract_intended_measurements_from_NSB(self) -> Optional[List[dict]]:
        """Get intended measurements metadata from NSB request form using the metadata-service."""
        response = requests.get(
            self.job_settings.metadata_service_path
            + f"/{self.job_settings.subject_id}"
        )
        response.raise_for_status()
        if response.status_code <= 300 or response.status_code == 406:
            json_content = response.json()
            return json_content["data"]
        else:
            logging.warning(
                f"Intended measurements metadata is not valid! {response.status_code}"
            )
            return None
        
    def _extract_device_info_from_session(session: Session):
        """
        Extract device information from the session.data_streams section.
        Maps the correct detectors and filters for each emission path based on the rig definition.

        Args:
            session: The Session Pydantic model to extract from

        Returns:
            dict: Dictionary with device information keyed by emission name
        """
        # Initialize with device info specific to each emission path
        # TODO: Could this device_info dict be moved to models.py?
        device_info = {
            "red": {
                "light_source_name": DEFAULT_CHANNEL_PROPS["light_source_name"],
                "detector_name": DETECTOR_MAPPINGS["red"],
                "filter_names": FILTER_MAPPINGS["red"]
                + [EXCITATION_FILTER_MAPPINGS["red"]],
                "excitation_power": DEFAULT_CHANNEL_PROPS["excitation_power"],
            },
            "green": {
                "light_source_name": DEFAULT_CHANNEL_PROPS["light_source_name"],
                "detector_name": DETECTOR_MAPPINGS["green"],
                "filter_names": FILTER_MAPPINGS["green"]
                + [EXCITATION_FILTER_MAPPINGS["green"]],
                "excitation_power": DEFAULT_CHANNEL_PROPS["excitation_power"],
            },
            "isosbestic": {
                "light_source_name": DEFAULT_CHANNEL_PROPS["light_source_name"],
                "detector_name": DETECTOR_MAPPINGS["isosbestic"],
                "filter_names": FILTER_MAPPINGS["isosbestic"]
                + [EXCITATION_FILTER_MAPPINGS["isosbestic"]],
                "excitation_power": DEFAULT_CHANNEL_PROPS["excitation_power"],
            },
        }

        for stream in session.data_streams:
            # Process light sources
            if stream.light_sources:
                for light_source in stream.light_sources:
                    if light_source.name:
                        led_name = light_source.name
                        # Try to match LED to an emission based on wavelength in name
                        for wavelength_str, led_color in LED_WAVELENGTH_NAMES.items():
                            if wavelength_str in led_name:
                                # Map LED color to emission name
                                emission_name = None
                                if led_color == "yellow":
                                    emission_name = "red"
                                elif led_color == "blue":
                                    emission_name = "green"
                                elif led_color == "uv":
                                    emission_name = "isosbestic"

                                if emission_name:
                                    device_info[emission_name][
                                        "light_source_name"
                                    ] = led_name
                                    if (
                                        light_source.excitation_power is not None
                                    ):
                                        device_info[emission_name]["excitation_power"] = (
                                            light_source.excitation_power
                                        )
                                break

        return device_info
    
    @staticmethod
    def _create_channel_object(
        channel_name: str,
        color: str,
        measurement: str,
        device_info: dict,
        excitation_power: Optional[float] = None,
        excitation_power_unit: PowerUnit = PowerUnit.UW,  # Default unit
    ) -> Channel:
        """
        Create a new Channel object with the required properties using the Channel Pydantic model.
        Uses device info from session data if available, otherwise uses defaults.

        Args:
            channel_name: Name for the channel
            color: Color code (R, G, B, or Iso)
            measurement: Intended measurement value
            device_info: Device information to use for this color
            excitation_power: Optional excitation power value
            excitation_power_unit: Power unit for the excitation power

        Returns:
            Channel: A new Channel object with required fields
        """
        # Map color code to emission name
        emission_name = CHANNEL_COLOR_MAP[color]

        # Get the correct excitation wavelength for this emission
        excitation_wavelength = EMISSION_TO_EXCITATION_MAP.get(emission_name)

        # Get the correct emission wavelength for this channel
        emission_wavelength = EMISSION_WAVELENGTHS.get(emission_name)

        # Use excitation_power from parameter if provided, otherwise from device_info
        if excitation_power is None:
            excitation_power = (
                1.0
                if device_info["excitation_power"] is None
                else float(device_info["excitation_power"])
            )

        # Create a Channel object using the Pydantic model
        channel = Channel(
            channel_name=channel_name,
            intended_measurement=measurement if measurement else None,
            light_source_name=device_info["light_source_name"],
            filter_names=device_info["filter_names"],
            detector_name=device_info["detector_name"],
            excitation_wavelength=excitation_wavelength,
            excitation_power=excitation_power,
            excitation_power_unit=excitation_power_unit,  # Use the passed unit
            emission_wavelength=emission_wavelength,
            description=CHANNEL_DESCRIPTIONS[emission_name],
        )

        return channel

    def _integrate_intended_measurements(
            self, intended_measurements_data: List[dict], session: Session
    ) -> Session:
        """
Integrate intended measurements into the session model.

        Args:
            intended_measurements_data: List of dictionaries containing intended measurements.
            session: The Session Pydantic model to update.

        Returns:
            Updated Session model.
        """
        if not intended_measurements_data:
            return session

        try:
            check_fiber_indexing(intended_measurements_data)
        except ValueError as e:
            logging.error(f"Fiber indexing error: {e}.")
            return session
        device_info = self._extract_device_info_from_session(session)
        for stream_idx, stream in enumerate(session.data_streams):
            # Check if stream has fiber connections
            if getattr(stream, "fiber_connections", None) is None:
                stream.fiber_connections = []
            existing_connections = {} # Map of existing fiber connections by name
            normalized_fiber_map = {} # Maps normalized names to original names

            for fiber_connection in getattr(stream, "fiber_connections", []):
                if "fiber_name" in fiber_connection:
                    fiber_name = getattr(fiber_connection, "fiber_name", None)
                    normalized_name = normalize_fiber_name(fiber_name)

                    # store the mapping from normalized to original name
                    normalized_fiber_map[normalized_name] = fiber_name

                    # Group fiber connections by their normalized names
                    existing_connections.setdefault(normalized_name, []).append(fiber_connection)

            new_connections = [] # List of new fiber connections to be added
            for measurement_fiber_name, measurements in intended_measurements_data.items():
                # Normalize the fiber name for comparison
                normalized_fiber_name = normalize_fiber_name(measurement_fiber_name)
                actual_fiber_name = normalized_fiber_map.get(normalized_fiber_name, None)
                fiber_connections = existing_connections.get(normalized_fiber_name, [])
                if not fiber_connections:
                    # If we can find any other connection, copy default values from it
                    default_values = {
                        "patch_cord_name": "Patch Cord A",  # Default value
                        "output_power_unit": PowerUnit.UW,  # Default value
                    }
                    if any(existing_connections.values()):
                        # Get the first connection from any fiber
                        sample_conn = next(iter(existing_connections.values()))[0]
                        # Copy common fields
                        for field in ["patch_cord_name", "output_power_unit"]:
                            if field in sample_conn:
                                default_values[field] = sample_conn[field]

                    # Create a default connection
                    fiber_connections = [
                        {
                            "fiber_name": actual_fiber_name,
                            "patch_cord_name": default_values["patch_cord_name"],
                            "patch_cord_output_power": None,  # Will be set in the channel
                            "output_power_unit": default_values["output_power_unit"],
                        }
                    ]

                # Base connection properties to copy
                base_conn = fiber_connections[0]
                base_patch_cord_name = getattr(
                    base_conn, "patch_cord_name", "Unknown Patch Cord")
                # base_patch_cord_name = base_conn.get(
                #     "patch_cord_name", "Unknown Patch Cord"
                # )
                base_output_power_unit = getattr(
                    base_conn, "output_power_unit", PowerUnit.UW
                )
                # base_output_power_unit = base_conn.get(
                #     "output_power_unit", PowerUnit.UW
                # )  # Default if missing

                # Process each color for this fiber
                for color_code, measurement in measurements.items():
                    if not measurement:
                        continue

                    color_name = color_code.split("_")[
                        1
                    ]  # Extract R, G, B, or Iso from measurement_X
                    channel_name = f"{actual_fiber_name}_{CHANNEL_COLOR_MAP[color_name]}"

                    # Get excitation power from the fiber connection if it exists
                    excitation_power = None
                    if (
                        "patch_cord_output_power" in base_conn
                        and base_conn["patch_cord_output_power"] is not None
                    ):
                        excitation_power = float(base_conn["patch_cord_output_power"])

                    # Create channel configuration using Pydantic model
                    channel = self._create_channel_object(
                        channel_name,
                        color_name,
                        measurement,
                        device_info[CHANNEL_COLOR_MAP[color_name]],
                        excitation_power,
                        base_output_power_unit,  # Pass the unit
                    )

                    # Create a new connection with this channel using Pydantic model
                    new_conn = FiberConnectionConfig(
                        fiber_name=fiber_name,
                        patch_cord_name=patch_cord_name,
                        patch_cord_output_power=Decimal("0.0"),
                        output_power_unit=PowerUnit.UW,
                        channel=channel,
                    )
                    new_connections.append(new_conn)

                    logging.info(
                        f"Created connection for {actual_fiber_name} with {color_name} channel"
                    )

            # Find fibers that have no measurements (disconnected)
            # We need to check all fiber names in the existing connections
            # and add them as "disconnected" if they don't have measurements
            all_fiber_names = set()
            for fiber_name in normalized_fiber_map.values():
                # Extract fiber index if possible
                if extract_fiber_index(fiber_name) is not None:
                    all_fiber_names.add(fiber_name)

            # Add connections for all fibers that don't have measurements
            for fiber_name in all_fiber_names:
                normalized_name = normalize_fiber_name(fiber_name)
                if normalized_name not in {
                    normalize_fiber_name(name) for name in intended_measurements_data.keys()
                }:
                    # Get existing connection for this fiber if it exists
                    existing_conns = existing_connections.get(normalized_name, [])
                    if existing_conns:
                        # Use the existing connection's patch cord name
                        conn = existing_conns[0]
                        patch_cord_name = conn.get("patch_cord_name", "Patch Cord")
                    else:
                        # Use a default patch cord name
                        patch_cord_name = "Patch Cord"

                    # This fiber has no measurements, mark as disconnected
                    disconnected_conn = FiberConnectionConfig(
                        fiber_name="disconnected",
                        patch_cord_name=patch_cord_name,
                        patch_cord_output_power=Decimal("0.0"),
                        output_power_unit=PowerUnit.UW,
                            channel=Channel(
                                channel_name="disconnected",
                                intended_measurement=None,
                                light_source_name="None",
                                filter_names=["None"],
                                detector_name="None",
                                excitation_wavelength=300,  # Minimum allowed value
                                excitation_power=0,
                                emission_wavelength=300,  # Minimum allowed value
                                description="Not connected to any implanted fiber",
                            ),
                    )
                    new_connections.append(disconnected_conn)
                    logging.info(
                        f"Created disconnected connection for {fiber_name}"
                    )

            # Update the stream's fiber_connections
            stream.fiber_connections.extend(new_connections)

            # Make sure stream has the FIB modality
            if not hasattr(stream, "stream_modalities") or stream.stream_modalities is None:
                stream.stream_modalities = []
            stream.stream_modalities.extend(
                [Modality.FIB] if Modality.FIB not in stream.stream_modalities else [])

            # Add start and end times if missing
            # TODO: fix these datetime timezones? 
            if not hasattr(stream, " stream_start_time") or stream.stream_start_time is None:
                stream.stream_start_time = datetime.now(timezone.utc).isoformat()
            if not hasattr(stream, "stream_end_time") or stream.stream_end_time is None:
                stream.stream_end_time = datetime.now(timezone.utc).isoformat()

            # Add notes explaining the intended measurement structure
            notes = (
                "Fiber connections are repeated for fibers with multiple channels. "
                "Disconnected fibers are marked with fiber_name 'disconnected'."
            )
            stream["notes"] = (
                notes if "notes" not in stream else f"{stream['notes']} {notes}"
            )

            # Ensure other required fields for FIB modality exist
            if not hasattr(stream, "light_sources") or stream.light_sources is None:
                stream.light_sources = []
                for emission_name, info in device_info.items():
                    if (
                        info["light_source_name"]
                        != DEFAULT_CHANNEL_PROPS["light_source_name"]
                    ):
                        stream.light_sources.append(
                            LightEmittingDiodeConfig(
                                name=info["light_source_name"],
                                wavelength=info["excitation_power"] or 1.0,
                                power_unit=PowerUnit.MW,
                            )
                        )
            if not hasattr(stream, "detectors") or stream.detectors is None:
                unique_detectors = set(DETECTOR_MAPPINGS.values())
                stream.detectors = []
                for detector_name in unique_detectors:
                    stream["detectors"].append({"name": detector_name})

        return session


        

    def integrate_intended_measurements_OG(
        intended_measurements_data: List[dict], session: Session
    ) -> Session:
        """Integrate intended measurements into the session model."""
        
        # Mapping channel names to their measurement keys
        channel_definitions = [
            ("Red", "intended_measurement_R"),
            ("Green", "intended_measurement_G"),
            ("Blue", "intended_measurement_B"),
            ("Isobestic", "intended_measurement_Iso"),
        ]
        
        for measurement in intended_measurements_data:
            fiber_name = measurement.get("fiber_name")
            for stream in session.data_streams:
                for fiber_connection in stream.fiber_connections:
                    if fiber_connection.fiber_name != fiber_name:
                        continue
                    if getattr(fiber_connection, "channel", None) is None:
                        # Build channels only for non-None intended measurements
                        channels = [
                            Channel.model_construct(
                                channel_name=name,
                                intended_measurement=measurement.get(key)
                            )
                            for name, key in channel_definitions
                            if measurement.get(key) is not None
                        ]
                        # overwrites field with list
                        fiber_connection.channel = channels
                    else:
                        # TODO: do we need to handle if channels exist? 
                        pass
        return session

    def run_job(self) -> JobResponse:
        """Run the complete ETL job and return a JobResponse.

        This method orchestrates the full ETL process:
        1. Extracts metadata from files and settings
        2. Transforms the data into a valid Session object
        3. Saves the session metadata to the specified output location
        4. Verifies the output file was written correctly

        Returns
        -------
        JobResponse
            Object containing status code, message, and optional data.
            Status codes:
            - 200: Success
            - 406: Validation errors
            - 500: File writing errors

        Notes
        -----
        Uses the parent class's _load method which
        handles validation and writing.
        """
        fiber_data = self._extract()
        transformed_session = self._transform(fiber_data)
        job_response = self._load(
            transformed_session, self.job_settings.output_directory
        )
        return job_response

if __name__ == "__main__":
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = FIBEtl(job_settings=main_job_settings)
    etl.run_job()
