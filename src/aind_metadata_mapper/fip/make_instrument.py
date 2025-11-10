"""Script to interactively create an FIP ophys instrument.json file.

This script prompts for rig-specific information (rig ID, computer name, serial numbers)
and creates a complete instrument.json file. Serial numbers and computer names are
automatically loaded from previous instruments for the same rig ID if available.

Usage:
    First-time setup (create conda environment and install dependencies):
        conda create -n fip-instrument python=3.10 -y
        conda activate fip-instrument
        cd /path/to/aind-metadata-mapper
        pip install -e .

    Run the script:
        conda activate fip-instrument
        cd /path/to/aind-metadata-mapper
        python -m aind_metadata_mapper.fip.make_instrument

    The script will:
        1. Prompt for a Rig ID (defaults to "test")
        2. Load previous instrument data for that rig if available
        3. Prompt for computer name (defaults to system hostname or previous value)
        4. Prompt for serial numbers (defaults to previous values if available)
        5. Save the instrument to ~/instrument_store/{rig_id}/

Output:
    - ~/instrument_store/{rig_id}/instrument.json: Current instrument saved to instrument store
    - ~/instrument_store/{rig_id}/instrument_YYYYMMDD.json: Previous versions archived by date
"""

import socket
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import aind_data_schema.components.devices as d
import aind_data_schema.core.instrument as r
from aind_data_schema.components.connections import Connection
from aind_data_schema.components.coordinates import CoordinateSystemLibrary
from aind_data_schema.components.devices import Computer
from aind_data_schema.components.identifiers import Software
from aind_data_schema.components.measurements import Calibration
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import PowerUnit

from aind_metadata_mapper.instrument_store import get_instrument, save_instrument


def prompt_for_string(prompt: str, default: Optional[str] = None, required: bool = False) -> str:
    """Prompt user for a string value.

    Parameters
    ----------
    prompt : str
        The prompt message.
    default : Optional[str]
        Optional default value to display.
    required : bool
        If True and no default provided, require non-empty input.

    Returns
    -------
    str
        User input or default if provided and user presses enter.
    """
    while True:
        if default:
            full_prompt = f"{prompt} [{default}]: "
        else:
            full_prompt = f"{prompt}: "
        response = input(full_prompt).strip()
        if response:
            return response
        if default:
            return default
        if not required:
            return ""
        print("This field is required. Please enter a value.")


def extract_serial_number_by_name(instrument_data: dict, component_name: str) -> Optional[str]:
    """Extract serial number from a component in instrument data by name.

    Parameters
    ----------
    instrument_data : dict
        Instrument data loaded from JSON.
    component_name : str
        Name of the component to find.

    Returns
    -------
    Optional[str]
        Serial number if found, None otherwise.
    """
    if instrument_data is None:
        return None

    components = instrument_data.get("components", [])
    for component in components:
        if component.get("name") == component_name:
            return component.get("serial_number")
    return None


def extract_computer_name(instrument_data: dict) -> Optional[str]:
    """Extract computer name from instrument data.

    Parameters
    ----------
    instrument_data : dict
        Instrument data loaded from JSON.

    Returns
    -------
    Optional[str]
        Computer name if found, None otherwise.
    """
    if instrument_data is None:
        return None

    components = instrument_data.get("components", [])
    for component in components:
        if component.get("__class_name") == "Computer":
            return component.get("name")
    return None


def create_instrument(rig_id: str) -> tuple[r.Instrument, str]:
    """Create an FIP instrument interactively.

    Parameters
    ----------
    rig_id : str
        Rig identifier for loading previous instrument defaults.

    Returns
    -------
    tuple[r.Instrument, str]
        Tuple of (instrument, computer_name) for use in connections.
    """
    # Try to load previous instrument for this rig
    previous_instrument = get_instrument(rig_id)

    # Prompt for computer name with system default or previous value
    system_hostname = socket.gethostname()
    previous_computer_name = extract_computer_name(previous_instrument)
    computer_name_default = previous_computer_name or system_hostname
    computer_name = prompt_for_string("Computer name", computer_name_default)

    computer = Computer(name=computer_name)

    bonsai_software = Software(name="Bonsai", version="2.5")

    patch_cord = d.FiberPatchCord(
        name="Bundle Branching Fiber-optic Patch Cord",
        manufacturer=d.Organization.DORIC,
        model="BBP(4)_200/220/900-0.37_Custom_FCM-4xMF1.25",
        core_diameter=200,
        numerical_aperture=0.37,
    )

    light_source_1 = d.LightEmittingDiode(
        name="470nm LED",
        manufacturer=d.Organization.THORLABS,
        model="M470F3",
        wavelength=470,
    )

    light_source_2 = d.LightEmittingDiode(
        name="415nm LED",
        manufacturer=d.Organization.THORLABS,
        model="M415F3",
        wavelength=415,
    )

    light_source_3 = d.LightEmittingDiode(
        name="565nm LED",
        manufacturer=d.Organization.THORLABS,
        model="M565F3",
        wavelength=565,
    )

    # Extract serial number defaults from previous instrument
    detector_1_serial_default = extract_serial_number_by_name(previous_instrument, "Green CMOS")
    detector_2_serial_default = extract_serial_number_by_name(previous_instrument, "Red CMOS")
    objective_serial_default = extract_serial_number_by_name(previous_instrument, "Objective")

    # Prompt for serial numbers (required if no default)
    detector_1_serial = prompt_for_string(
        "Green CMOS serial number", detector_1_serial_default, required=detector_1_serial_default is None
    )
    detector_2_serial = prompt_for_string(
        "Red CMOS serial number", detector_2_serial_default, required=detector_2_serial_default is None
    )
    objective_serial = prompt_for_string(
        "Objective serial number", objective_serial_default, required=objective_serial_default is None
    )

    detector_1 = d.Detector(
        name="Green CMOS",
        serial_number=detector_1_serial,
        manufacturer=d.Organization.FLIR,
        model="BFS-U3-20S40M",
        detector_type="Camera",
        data_interface="USB",
        cooling="Air",
        immersion="air",
        bin_width=4,
        bin_height=4,
        bin_mode="Additive",
        crop_offset_x=0,
        crop_offset_y=0,
        crop_width=200,
        crop_height=200,
        gain=2,
        chroma="Monochrome",
        bit_depth=16,
        recording_software=bonsai_software,
    )

    detector_2 = d.Detector(
        name="Red CMOS",
        serial_number=detector_2_serial,
        manufacturer=d.Organization.FLIR,
        model="BFS-U3-20S40M",
        detector_type="Camera",
        data_interface="USB",
        cooling="Air",
        immersion="air",
        bin_width=4,
        bin_height=4,
        bin_mode="Additive",
        crop_offset_x=0,
        crop_offset_y=0,
        crop_width=200,
        crop_height=200,
        gain=2,
        chroma="Monochrome",
        bit_depth=16,
        recording_software=bonsai_software,
    )

    objective = d.Objective(
        name="Objective",
        serial_number=objective_serial,
        manufacturer=d.Organization.NIKON,
        model="CFI Plan Apochromat Lambda D 10x",
        numerical_aperture=0.45,
        magnification=10,
        immersion="air",
    )

    filter_1 = d.Filter(
        name="Green emission filter",
        manufacturer=d.Organization.SEMROCK,
        model="FF01-520/35-25",
        filter_type="Band pass",
        center_wavelength=520,
    )

    filter_2 = d.Filter(
        name="Red emission filter",
        manufacturer=d.Organization.SEMROCK,
        model="FF01-600/37-25",
        filter_type="Band pass",
        center_wavelength=600,
    )

    filter_3 = d.Filter(
        name="Emission Dichroic",
        model="FF562-Di03-25x36",
        manufacturer=d.Organization.SEMROCK,
        filter_type="Dichroic",
        cut_off_wavelength=562,
    )

    filter_4 = d.Filter(
        name="dual-edge standard epi-fluorescence dichroic beamsplitter",
        model="FF493/574-Di01-25x36",
        manufacturer=d.Organization.SEMROCK,
        notes="BrightLine dual-edge standard epi-fluorescence dichroic beamsplitter",
        filter_type="Multiband",
        center_wavelength=[493, 574],
    )

    filter_5 = d.Filter(
        name="Excitation filter 410nm",
        manufacturer=d.Organization.THORLABS,
        model="FB410-10",
        filter_type="Band pass",
        center_wavelength=410,
    )

    filter_6 = d.Filter(
        name="Excitation filter 470nm",
        manufacturer=d.Organization.THORLABS,
        model="FB470-10",
        filter_type="Band pass",
        center_wavelength=470,
    )

    filter_7 = d.Filter(
        name="Excitation filter 560nm",
        manufacturer=d.Organization.THORLABS,
        model="FB560-10",
        filter_type="Band pass",
        center_wavelength=560,
    )

    filter_8 = d.Filter(
        name="450 Dichroic Longpass Filter",
        manufacturer=d.Organization.EDMUND_OPTICS,
        model="#69-898",
        filter_type="Dichroic",
        cut_off_wavelength=450,
    )

    filter_9 = d.Filter(
        name="500 Dichroic Longpass Filter",
        manufacturer=d.Organization.EDMUND_OPTICS,
        model="#69-899",
        filter_type="Dichroic",
        cut_off_wavelength=500,
    )

    lens = d.Lens(
        manufacturer=d.Organization.THORLABS,
        model="AC254-080-A-ML",
        name="Image focusing lens",
    )

    daq = d.HarpDevice(
        name="Harp Behavior",
        harp_device_type=d.HarpDeviceType.BEHAVIOR,
        core_version="2.1",
        is_clock_generator=False,
        channels=[
            d.DAQChannel(channel_name="DO0", channel_type="Digital Output"),
            d.DAQChannel(channel_name="DO1", channel_type="Digital Output"),
            d.DAQChannel(channel_name="DI0", channel_type="Digital Input"),
            d.DAQChannel(channel_name="DI1", channel_type="Digital Input"),
            d.DAQChannel(channel_name="DI3", channel_type="Digital Input"),
        ],
    )

    connections = [
        Connection(
            source_device="Photometry Clock",
            target_device="Harp Behavior",
            target_port="DI3",
        ),
        Connection(
            source_device="Harp Behavior",
            target_device=computer_name,
        ),
    ]

    photemetry_clock = d.Device(name="Photometry Clock")

    calibration = Calibration(
        calibration_date=datetime(2023, 10, 2, 3, 15, 22, tzinfo=timezone.utc),
        device_name="470nm LED",
        description="LED calibration",
        input=[1, 2, 3],
        input_unit=PowerUnit.PERCENT,
        output=[5, 10, 13],
        output_unit=PowerUnit.MW,
        measured_at="patch cord end",
    )

    instrument = r.Instrument(
        location="428",
        instrument_id="FIP1",
        modification_date=date.today(),
        modalities=[Modality.FIB],
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        components=[
            computer,
            patch_cord,
            light_source_1,
            light_source_2,
            light_source_3,
            detector_1,
            detector_2,
            objective,
            filter_1,
            filter_2,
            filter_3,
            filter_4,
            filter_5,
            filter_6,
            filter_7,
            filter_8,
            filter_9,
            lens,
            daq,
            photemetry_clock,
        ],
        calibrations=[calibration],
        connections=connections,
    )

    return instrument, computer_name


if __name__ == "__main__":
    # Prompt for rig_id first
    rig_id = prompt_for_string("Rig ID", "test")

    # Create instrument interactively
    instrument, _ = create_instrument(rig_id)

    # Write to temporary file, then save to instrument store
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        f.write(instrument.model_dump_json(indent=2))
        temp_path = f.name

    # Save to instrument store using the rig_id we prompted for
    stored_path = save_instrument(path=temp_path, rig_id=rig_id)
    print(f"Instrument saved to store: {stored_path}")

    # Clean up temporary file
    Path(temp_path).unlink()
