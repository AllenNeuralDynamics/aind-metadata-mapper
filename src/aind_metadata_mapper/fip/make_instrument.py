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
        1. Prompt for an Instrument ID (required, no default)
        2. Load previous instrument data for that ID if available from database
        3. Prompt for computer name (defaults to system hostname or previous value)
        4. Prompt for serial numbers (defaults to previous values if available)
        5. POST the instrument to the database and verify with a round-trip test
"""

import logging
import socket
import sys
from datetime import date
from typing import Optional

import aind_data_schema.components.devices as devices
import aind_data_schema.core.instrument as instrument
from aind_data_schema.components.connections import Connection
from aind_data_schema.components.devices import Computer
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.utils import (
    check_existing_instrument,
    check_instrument_id,
    prompt_for_string,
    save_instrument,
)

logger = logging.getLogger(__name__)


def create_instrument(
    instrument_id: str,
    location: Optional[str] = None,
    computer_name: Optional[str] = None,
    detector_1_serial: Optional[str] = None,
    detector_2_serial: Optional[str] = None,
    objective_serial: Optional[str] = None,
    previous_instrument: Optional[dict] = None,
    input_func=input,
) -> instrument.Instrument:
    """Create an FIP instrument interactively.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier.
    location : Optional[str]
        Location. If None, will prompt (optional).
    computer_name : Optional[str]
        Computer name. If None, defaults to system hostname or prompts.
    detector_1_serial : Optional[str]
        Green CMOS serial number. If None, will prompt (required if no previous instrument).
    detector_2_serial : Optional[str]
        Red CMOS serial number. If None, will prompt (required if no previous instrument).
    objective_serial : Optional[str]
        Objective serial number. If None, will prompt (required if no previous instrument).
    previous_instrument : Optional[dict]
        Optional previous instrument data as dict. Used for defaults.

    Returns
    -------
    instrument.Instrument
        Created instrument object.

    Notes
    -----
    Prompted values (if parameter is None):
        - location (optional, can be empty)
        - computer_name (defaults to system hostname if not provided)
        - detector_1_serial (Green CMOS serial number, required if no previous instrument)
        - detector_2_serial (Red CMOS serial number, required if no previous instrument)
        - objective_serial (required if no previous instrument)

    Hard-coded values (must be updated in this file if changed):
        - All device specifications (patch cords, LEDs, detectors, objective, filters, lens, harp devices)
        - Detector settings (bin_width=4, bin_height=4, crop offsets, gain=0, etc.)
        - Connection details (source_port="COM14", etc.)
        - Coordinate system (bregma, matching behavior)
        - Modality (FIB)
        - modification_date (date.today())
    """
    # Get previous instrument components for defaults
    previous_instrument_obj = None
    if previous_instrument:
        previous_instrument_obj = instrument.Instrument.model_validate(previous_instrument)

    components_by_name = {}
    if previous_instrument_obj:
        components_by_name = {
            getattr(component, "name", ""): component
            for component in previous_instrument_obj.components
            if getattr(component, "name", None)
        }

    # Get defaults from previous instrument or system
    defaults = {
        "location": previous_instrument_obj.location if previous_instrument_obj else None,
        "computer_name": socket.gethostname(),
        "detector_1_serial": getattr(components_by_name.get("Green CMOS"), "serial_number", None),
        "detector_2_serial": getattr(components_by_name.get("Red CMOS"), "serial_number", None),
        "objective_serial": getattr(components_by_name.get("Objective"), "serial_number", None),
    }

    # Use provided values or prompt for missing ones
    if location is None:
        location = prompt_for_string("Location", defaults["location"], required=False, input_func=input_func)
    if computer_name is None:
        computer_name = prompt_for_string(
            "Computer name", defaults["computer_name"], required=False, input_func=input_func
        )
    if detector_1_serial is None:
        required = defaults["detector_1_serial"] is None
        detector_1_serial = prompt_for_string(
            "Green CMOS serial number", defaults["detector_1_serial"], required=required, input_func=input_func
        )
    if detector_2_serial is None:
        required = defaults["detector_2_serial"] is None
        detector_2_serial = prompt_for_string(
            "Red CMOS serial number", defaults["detector_2_serial"], required=required, input_func=input_func
        )
    if objective_serial is None:
        required = defaults["objective_serial"] is None
        objective_serial = prompt_for_string(
            "Objective serial number", defaults["objective_serial"], required=required, input_func=input_func
        )

    # Use defaults for empty strings
    if not computer_name:
        computer_name = socket.gethostname()
    location = location if location else None

    computer = Computer(name=computer_name)

    patch_cord_note = (
        "All four patch cords are actually a single device at the camera end "
        "with four separate connections to connect to up to four implanted fibers. "
        "Unused patch cables are not physically connected to an implanted fiber during an experiment."
    )

    patch_cord_0 = devices.FiberPatchCord(
        name="Patch Cord 0",
        manufacturer=devices.Organization.DORIC,
        model="BBP(4)_200/220/900-0.37_Custom_FCM-4xMF1.25",
        core_diameter=200,
        numerical_aperture=0.37,
        notes=patch_cord_note,
    )

    patch_cord_1 = devices.FiberPatchCord(
        name="Patch Cord 1",
        manufacturer=devices.Organization.DORIC,
        model="BBP(4)_200/220/900-0.37_Custom_FCM-4xMF1.25",
        core_diameter=200,
        numerical_aperture=0.37,
        notes=patch_cord_note,
    )

    patch_cord_2 = devices.FiberPatchCord(
        name="Patch Cord 2",
        manufacturer=devices.Organization.DORIC,
        model="BBP(4)_200/220/900-0.37_Custom_FCM-4xMF1.25",
        core_diameter=200,
        numerical_aperture=0.37,
        notes=patch_cord_note,
    )

    patch_cord_3 = devices.FiberPatchCord(
        name="Patch Cord 3",
        manufacturer=devices.Organization.DORIC,
        model="BBP(4)_200/220/900-0.37_Custom_FCM-4xMF1.25",
        core_diameter=200,
        numerical_aperture=0.37,
        notes=patch_cord_note,
    )

    light_source_1 = devices.LightEmittingDiode(
        name="470nm LED",
        manufacturer=devices.Organization.THORLABS,
        model="M470F3",
        wavelength=470,
    )

    light_source_2 = devices.LightEmittingDiode(
        name="415nm LED",
        manufacturer=devices.Organization.THORLABS,
        model="M415F3",
        wavelength=415,
    )

    light_source_3 = devices.LightEmittingDiode(
        name="565nm LED",
        manufacturer=devices.Organization.THORLABS,
        model="M565F3",
        wavelength=565,
    )

    detector_1 = devices.Detector(
        name="Green CMOS",
        serial_number=detector_1_serial,
        manufacturer=devices.Organization.FLIR,
        model="BFS-U3-20S40M",
        detector_type="Camera",
        data_interface="USB",
        cooling="Air",
        immersion="air",
        bin_width=4,
        bin_height=4,
        bin_mode="Additive",
        crop_offset_x=104,
        crop_offset_y=56,
        crop_width=200,
        crop_height=200,
        gain=0,
        chroma="Monochrome",
        bit_depth=16,
    )

    detector_2 = devices.Detector(
        name="Red CMOS",
        serial_number=detector_2_serial,
        manufacturer=devices.Organization.FLIR,
        model="BFS-U3-20S40M",
        detector_type="Camera",
        data_interface="USB",
        cooling="Air",
        immersion="air",
        bin_width=4,
        bin_height=4,
        bin_mode="Additive",
        crop_offset_x=76,
        crop_offset_y=56,
        crop_width=200,
        crop_height=200,
        gain=0,
        chroma="Monochrome",
        bit_depth=16,
    )

    objective = devices.Objective(
        name="Objective",
        serial_number=objective_serial,
        manufacturer=devices.Organization.NIKON,
        model="CFI Plan Apochromat Lambda D 10x",
        numerical_aperture=0.45,
        magnification=10,
        immersion="air",
    )

    filter_1 = devices.Filter(
        name="Green emission filter",
        manufacturer=devices.Organization.SEMROCK,
        model="FF01-520/35-25",
        filter_type="Band pass",
        center_wavelength=520,
    )

    filter_2 = devices.Filter(
        name="Red emission filter",
        manufacturer=devices.Organization.SEMROCK,
        model="FF01-600/37-25",
        filter_type="Band pass",
        center_wavelength=600,
    )

    filter_3 = devices.Filter(
        name="Emission Dichroic",
        model="FF562-Di03-25x36",
        manufacturer=devices.Organization.SEMROCK,
        filter_type="Dichroic",
        cut_off_wavelength=562,
    )

    filter_4 = devices.Filter(
        name="dual-edge standard epi-fluorescence dichroic beamsplitter",
        model="FF493/574-Di01-25x36",
        manufacturer=devices.Organization.SEMROCK,
        notes="BrightLine dual-edge standard epi-fluorescence dichroic beamsplitter",
        filter_type="Multiband",
        center_wavelength=[493, 574],
    )

    filter_5 = devices.Filter(
        name="Excitation filter 410nm",
        manufacturer=devices.Organization.THORLABS,
        model="FB410-10",
        filter_type="Band pass",
        center_wavelength=410,
    )

    filter_6 = devices.Filter(
        name="Excitation filter 470nm",
        manufacturer=devices.Organization.THORLABS,
        model="FB470-10",
        filter_type="Band pass",
        center_wavelength=470,
    )

    filter_7 = devices.Filter(
        name="Excitation filter 560nm",
        manufacturer=devices.Organization.THORLABS,
        model="FB560-10",
        filter_type="Band pass",
        center_wavelength=560,
    )

    filter_8 = devices.Filter(
        name="450 Dichroic Longpass Filter",
        manufacturer=devices.Organization.EDMUND_OPTICS,
        model="#69-898",
        filter_type="Dichroic",
        cut_off_wavelength=450,
    )

    filter_9 = devices.Filter(
        name="500 Dichroic Longpass Filter",
        manufacturer=devices.Organization.EDMUND_OPTICS,
        model="#69-899",
        filter_type="Dichroic",
        cut_off_wavelength=500,
    )

    lens = devices.Lens(
        manufacturer=devices.Organization.THORLABS,
        model="AC254-080-A-ML",
        name="Image focusing lens",
    )

    cuttlefish = devices.HarpDevice(
        name="cuTTLefishFip",
        harp_device_type=devices.HarpDeviceType.CUTTLEFISHFIP,
        is_clock_generator=False,
        data_interface=devices.DataInterface.USB,
    )

    white_rabbit = devices.HarpDevice(
        name="harp_clock_generator",
        harp_device_type=devices.HarpDeviceType.WHITERABBIT,
        manufacturer=devices.Organization.AIND,
        is_clock_generator=True,
        channels=[
            devices.DAQChannel(channel_name="ClkOut", channel_type=devices.DaqChannelType.DO),
        ],
    )

    connections = [
        Connection(
            source_device="cuTTLefishFip",
            source_port="COM14",
            target_device=computer_name,
        ),
        Connection(
            source_device="harp_clock_generator",
            source_port="ClkOut",
            target_device="cuTTLefishFip",
            target_port="COM14",
            send_and_receive=False,
        ),
    ]

    # Coordinate system matching behavior (bregma with X/Y/Z axes, not BREGMA_ARI)
    coordinate_system = {
        "object_type": "Coordinate system",
        "name": "origin",
        "origin": "Bregma",
        "axes": [
            {"object_type": "Axis", "name": "X", "direction": "Left_to_right"},
            {"object_type": "Axis", "name": "Y", "direction": "Anterior_to_posterior"},
            {"object_type": "Axis", "name": "Z", "direction": "Inferior_to_superior"},
        ],
        "axis_unit": "millimeter",
    }

    instrument_model = instrument.Instrument(
        location=location if location else None,
        instrument_id=instrument_id,
        modification_date=date.today(),
        modalities=[Modality.FIB],
        coordinate_system=coordinate_system,
        components=[
            computer,
            patch_cord_0,
            patch_cord_1,
            patch_cord_2,
            patch_cord_3,
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
            cuttlefish,
            white_rabbit,
        ],
        calibrations=[],
        connections=connections,
    )

    return instrument_model


def main(
    instrument_id: str,
    skip_confirmation: bool = False,
    input_func=input,
) -> None:
    """Main function to create an instrument interactively.

    Parameters
    ----------
    instrument_id : str
        Instrument ID (required).
    skip_confirmation : bool
        If True, skip confirmation prompt for new instrument IDs.
    """
    # Check if instrument exists and get previous instrument data
    previous_instrument = check_instrument_id(
        instrument_id=instrument_id,
        skip_confirmation=skip_confirmation,
        input_func=input_func,
    )

    # Create instrument interactively
    instrument_model = create_instrument(
        instrument_id,
        previous_instrument=previous_instrument,
        input_func=input_func,
    )

    # Check if instrument with same ID and date already exists
    modification_date_str = instrument_model.modification_date.isoformat()
    record_exists = check_existing_instrument(instrument_id, modification_date_str)
    replace = False
    if record_exists:
        logger.info(
            f"An instrument with ID '{instrument_id}' and modification_date '{modification_date_str}' already exists."
        )
        response = input_func("Do you want to overwrite the existing record? [y/N]: ").strip().lower()
        if response in ("y", "yes"):
            replace = True
            logger.info("Will overwrite existing record.")
        else:
            logger.info("Cancelled. Not overwriting existing record.")
            sys.exit(0)

    # Save instrument (includes round-trip validation)
    try:
        save_instrument(instrument_model, replace=replace)
    except ValueError as e:
        logger.error(f"Failed - {e}")
        sys.exit(1)


if __name__ == "__main__":
    instrument_id = prompt_for_string("Instrument ID", required=True)
    main(instrument_id=instrument_id)
