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

import json
import socket
import sys
from datetime import date
from typing import Optional, Union

import aind_data_schema.components.devices as devices
import aind_data_schema.core.instrument as instrument
import requests
from aind_data_schema.components.connections import Connection
from aind_data_schema.components.coordinates import CoordinateSystemLibrary
from aind_data_schema.components.devices import Computer
from aind_data_schema_models.modalities import Modality

API_BASE_URL = "http://aind-metadata-service-dev/api/v2/instrument"


def get_instrument_from_db(instrument_id: str) -> Optional[dict]:
    """Get latest instrument from database.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier.

    Returns
    -------
    Optional[dict]
        Instrument data as dict, or None if not found.
    """
    response = requests.get(
        f"{API_BASE_URL}/{instrument_id}",
        params={"partial_match": True},
    )
    if response.status_code == 404:
        return None
    # GET 400 returns valid JSON data (API quirk)
    records = response.json()
    if not records:
        return None
    # Find latest record matching instrument_id
    matching_records = [r for r in records if r.get("modification_date") and r.get("instrument_id") == instrument_id]
    if not matching_records:
        return None
    latest_record = sorted(matching_records, key=lambda record: record["modification_date"])[-1]
    return latest_record


def list_instrument_ids_from_db() -> list[str]:
    """List all instrument IDs in the database.

    Returns
    -------
    list[str]
        List of instrument IDs.
    """
    # Get all unique instrument_ids from the database
    # This is a simplified approach - in practice you might need a different endpoint
    instrument_ids = set()
    # We can't easily list all IDs without a dedicated endpoint, so return empty list
    # This could be enhanced if the API provides a list endpoint
    return sorted(instrument_ids)


def get_latest_instrument_from_db(instrument_id: str, original_instrument: instrument.Instrument) -> None:
    """GET latest instrument from database and validate round-trip.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier.
    original_instrument : instrument.Instrument
        Original instrument that was POSTed, for round-trip validation.

    Raises
    ------
    ValueError
        If instrument not found in database or round-trip validation fails.
    """
    print(f"GETting instrument from {API_BASE_URL}/{instrument_id} to verify that POST was successful")
    response = requests.get(
        f"{API_BASE_URL}/{instrument_id}",
        params={"partial_match": True},
    )
    # GET 400 returns valid JSON data (API quirk), but 404 is a real error
    if response.status_code == 404:
        raise ValueError(f"Instrument '{instrument_id}' not found in database")
    # Parse JSON (works for both 200 and 400 status codes)
    records = response.json()

    # Find latest record matching instrument_id
    matching_records = [r for r in records if r.get("modification_date") and r.get("instrument_id") == instrument_id]
    if not matching_records:
        raise ValueError(f"Instrument '{instrument_id}' not found in database")
    latest_record = sorted(matching_records, key=lambda record: record["modification_date"])[-1]

    # Validate round-trip
    read_back_instrument = instrument.Instrument.model_validate(latest_record)
    if validate_round_trip(original_instrument, read_back_instrument):
        print("Instrument.json successfully stored in the db")
    else:
        raise ValueError("Round-trip test failed: Source and read-back instruments differ")


def post_instrument_to_db(instrument_model: instrument.Instrument) -> None:
    """POST instrument to database.

    Parameters
    ----------
    instrument_model : instrument.Instrument
        Instrument to POST.

    Raises
    ------
    ValueError
        If POST fails (e.g., record already exists).
    requests.HTTPError
        If server error occurs (500+).
    """
    # Use model_dump_json() and parse to ensure dates are properly serialized
    source_dict = json.loads(instrument_model.model_dump_json())
    print(f"POSTing instrument to {API_BASE_URL}")
    response = requests.post(API_BASE_URL, json=source_dict)
    # POST 400 is always an error (e.g., "Record already exists")
    if response.status_code == 400:
        try:
            error_msg = response.json().get("message", response.text)
        except json.JSONDecodeError:
            error_msg = response.text
        raise ValueError(f"Cannot POST instrument: {error_msg}")
    if response.status_code >= 500:
        response.raise_for_status()


def get_instrument_id_and_previous(
    instrument_id: Optional[str] = None,
    skip_confirmation: bool = False,
    input_func=input,
) -> tuple[str, Optional[dict]]:
    """Get instrument ID and previous instrument data with confirmation.

    Parameters
    ----------
    instrument_id : Optional[str]
        Optional instrument ID. If None, will prompt user.
    skip_confirmation : bool
        If True, skip confirmation prompt for new instrument IDs.
    input_func : callable
        Function to get user input. Defaults to builtin input().

    Returns
    -------
    tuple[str, Optional[dict]]
        Tuple of (instrument_id, previous_instrument_dict or None).

    Raises
    ------
    SystemExit
        If user cancels creation of new instrument ID.
    """
    # Get list of existing instrument IDs for help message
    existing_ids = list_instrument_ids_from_db()
    if existing_ids:
        help_message = "Below is a list of existing instrument IDs:\n" + "\n".join(f"  - {id}" for id in existing_ids)
    else:
        help_message = "No existing instrument IDs found in database."

    # Get instrument_id from parameter or prompt
    if instrument_id is None:
        instrument_id = prompt_for_string(
            "Instrument ID", required=True, help_message=help_message, input_func=input_func
        )

    # Check if instrument exists, and if not, confirm creation of new ID
    previous_instrument = get_instrument_from_db(instrument_id)
    if previous_instrument is None and not skip_confirmation:
        print("This is a new instrument ID.")
        if existing_ids:
            print("Here is a list of existing instrument IDs:")
            for id in existing_ids:
                print(f"  - {id}")
        else:
            print("No existing instrument IDs found in database.")
        print()
        if not prompt_yes_no(
            f"Are you sure you want to create a new ID with name '{instrument_id}'?",
            default=True,
            input_func=input_func,
        ):
            print("Cancelled. Please run the script again with the correct instrument ID.")
            sys.exit(0)

    return instrument_id, previous_instrument


def validate_round_trip(original: instrument.Instrument, retrieved: instrument.Instrument) -> bool:
    """Validate that retrieved instrument matches original.

    Parameters
    ----------
    original : instrument.Instrument
        Original instrument that was POSTed.
    retrieved : instrument.Instrument
        Instrument retrieved from database.

    Returns
    -------
    bool
        True if instruments match, False otherwise.
    """
    # Use model_dump_json() and parse to ensure consistent serialization
    source_dict = json.loads(original.model_dump_json())
    read_back_dict = json.loads(retrieved.model_dump_json())
    return source_dict == read_back_dict


def prompt_yes_no(prompt: str, default: bool = True, input_func=input) -> bool:
    """Prompt user for yes/no response.

    Parameters
    ----------
    prompt : str
        The prompt message.
    default : bool
        Default value if user presses Enter. Defaults to True (yes).
    input_func : callable
        Function to get user input. Defaults to builtin input().

    Returns
    -------
    bool
        True for yes, False for no.
    """
    default_str = "Y/n" if default else "y/N"
    full_prompt = f"{prompt} [{default_str}]: "
    response = input_func(full_prompt).strip().lower()

    if not response:
        return default
    return response in ("y", "yes")


def prompt_for_string(
    prompt: str,
    default: Optional[str] = None,
    required: bool = False,
    help_message: Optional[str] = None,
    input_func=input,
) -> str:
    """Prompt user for a string value.

    Parameters
    ----------
    prompt : str
        The prompt message.
    default : Optional[str]
        Optional default value to display.
    required : bool
        If True and no default provided, require non-empty input.
    help_message : Optional[str]
        Optional help message to display when required field is empty.
    input_func : callable
        Function to get user input. Defaults to builtin input().

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
        response = input_func(full_prompt).strip()
        if response:
            return response
        if default:
            return default
        if not required:
            return ""
        print("This field is required. Please enter a value.")
        if help_message:
            print(help_message)


def create_instrument(
    instrument_id: str,
    values: Optional[dict] = None,
    previous_instrument: Optional[Union[dict, instrument.Instrument]] = None,
    input_func=input,
) -> instrument.Instrument:
    """Create an FIP instrument interactively.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier for loading previous instrument defaults.
    values : Optional[dict]
        Optional dict with keys: location, computer_name, detector_1_serial,
        detector_2_serial, objective_serial. If provided, bypasses prompts.
    previous_instrument : Optional[Union[dict, instrument.Instrument]]
        Optional previous instrument data (dict or validated Instrument object).
        If None, will be loaded from database.

    Returns
    -------
    instrument.Instrument
        Created instrument object.
    """
    # Load and validate previous instrument if not provided
    previous_instrument_obj = None
    if previous_instrument is None:
        instrument_dict = get_instrument_from_db(instrument_id)
        if instrument_dict is not None:
            previous_instrument_obj = instrument.Instrument.model_validate(instrument_dict)
    elif isinstance(previous_instrument, dict):
        previous_instrument_obj = instrument.Instrument.model_validate(previous_instrument)
    elif isinstance(previous_instrument, instrument.Instrument):
        previous_instrument_obj = previous_instrument

    components_by_name = {}
    if previous_instrument_obj:
        components_by_name = {
            getattr(component, "name", ""): component
            for component in previous_instrument_obj.components
            if getattr(component, "name", None)
        }

    defaults = {
        "location": previous_instrument_obj.location if previous_instrument_obj else None,
        "computer_name": socket.gethostname(),
        "detector_1_serial": getattr(components_by_name.get("Green CMOS"), "serial_number", None),
        "detector_2_serial": getattr(components_by_name.get("Red CMOS"), "serial_number", None),
        "objective_serial": getattr(components_by_name.get("Objective"), "serial_number", None),
    }

    user_values: dict = {} if values is None else {**values}
    if values is None:
        prompts = (
            ("location", "Location", False),
            ("computer_name", "Computer name", False),
            ("detector_1_serial", "Green CMOS serial number", True),
            ("detector_2_serial", "Red CMOS serial number", True),
            ("objective_serial", "Objective serial number", True),
        )
        for key, label, require_if_missing in prompts:
            default_value = defaults[key]
            required = require_if_missing and default_value is None
            user_values[key] = prompt_for_string(label, default_value, required=required, input_func=input_func)
    else:
        for key, fallback in defaults.items():
            if not user_values.get(key) and fallback is not None:
                user_values[key] = fallback

    location = user_values.get("location", "")
    computer_name = user_values.get("computer_name", socket.gethostname())
    detector_1_serial = user_values.get("detector_1_serial", "")
    detector_2_serial = user_values.get("detector_2_serial", "")
    objective_serial = user_values.get("objective_serial", "")

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

    instrument_model = instrument.Instrument(
        location=location if location else None,
        instrument_id=instrument_id,
        modification_date=date.today(),
        modalities=[Modality.FIB],
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
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
    instrument_id: Optional[str] = None,
    values: Optional[dict] = None,
    skip_confirmation: bool = False,
    input_func=input,
) -> None:
    """Main function to create an instrument interactively.

    Parameters
    ----------
    instrument_id : Optional[str]
        Optional instrument ID. If None, will prompt user.
    values : Optional[dict]
        Optional dict with values to bypass prompts. See create_instrument() for keys.
    skip_confirmation : bool
        If True, skip confirmation prompt for new instrument IDs.
    """
    # Get instrument ID and previous instrument data
    instrument_id, previous_instrument = get_instrument_id_and_previous(
        instrument_id=instrument_id,
        skip_confirmation=skip_confirmation,
        input_func=input_func,
    )

    # Create instrument interactively
    instrument_model = create_instrument(
        instrument_id,
        values=values,
        previous_instrument=previous_instrument,
        input_func=input_func,
    )

    # POST to database
    try:
        post_instrument_to_db(instrument_model)
    except ValueError as e:
        print(f"Failed - {e}")
        sys.exit(1)

    # GET from database and validate round-trip
    try:
        get_latest_instrument_from_db(instrument_id, instrument_model)
    except ValueError as e:
        print(f"Failed - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
