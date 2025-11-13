"""Script to create a minimal FIP ophys instrument.json file.

This script creates the absolute minimal instrument.json file that still validates,
with all optional fields removed. This is useful for understanding what the bare
minimum required fields are.

Usage:
    python -m aind_metadata_mapper.fip.make_instrument_minimal

Output:
    - src/aind_metadata_mapper/fip/instrument_fip_minimal.json: Minimal instrument JSON
"""

import socket
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import aind_data_schema.components.devices as devices
import aind_data_schema.core.instrument as instrument
from aind_data_schema.components.connections import Connection
from aind_data_schema.components.coordinates import CoordinateSystemLibrary
from aind_data_schema.components.devices import Computer
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.instrument_store import get_instrument, list_instrument_ids, save_instrument


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


def _get_defaults_from_previous_instrument(
    previous_instrument_obj: Optional[instrument.Instrument],
) -> tuple[str, Optional[str]]:
    """Get default computer name and location from previous instrument.

    Parameters
    ----------
    previous_instrument_obj : Optional[instrument.Instrument]
        Previous instrument object, if available.

    Returns
    -------
    tuple[str, Optional[str]]
        Tuple of (default_computer_name, default_location).
    """
    if previous_instrument_obj:
        # Try to find computer component
        computer_component = None
        for component in previous_instrument_obj.components:
            if isinstance(component, Computer):
                computer_component = component
                break
        default_computer_name = computer_component.name if computer_component else socket.gethostname()
        default_location = previous_instrument_obj.location
    else:
        default_computer_name = socket.gethostname()
        default_location = None
    return default_computer_name, default_location


def _confirm_new_instrument_id(
    instrument_id: str, existing_ids: list[str], skip_confirmation: bool, input_func=input
) -> None:
    """Confirm creation of new instrument ID.

    Parameters
    ----------
    instrument_id : str
        Instrument ID to confirm.
    existing_ids : list[str]
        List of existing instrument IDs.
    skip_confirmation : bool
        If True, skip confirmation.
    input_func : callable
        Function to get user input.

    Raises
    ------
    SystemExit
        If user cancels.
    """
    if skip_confirmation:
        return

    print("This is a new instrument ID.")
    if existing_ids:
        print("Here is a list of existing instrument IDs:")
        for id in existing_ids:
            print(f"  - {id}")
    else:
        print("No existing instrument IDs found in store.")
    print()
    if not prompt_yes_no(
        f"Are you sure you want to create a new ID with name '{instrument_id}'?",
        default=True,
        input_func=input_func,
    ):
        print("Cancelled. Please run the script again with the correct instrument ID.")
        sys.exit(0)


def create_minimal_instrument(
    instrument_id: str, computer_name: str = None, location: Optional[str] = None
) -> instrument.Instrument:
    """Create a minimal FIP instrument with only required fields.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier.
    computer_name : str, optional
        Computer name. Defaults to system hostname.
    location : str, optional
        Location/room name. If None, will not be set in instrument.

    Returns
    -------
    instrument.Instrument
        Created minimal instrument object.
    """
    if computer_name is None:
        computer_name = socket.gethostname()

    computer = Computer(name=computer_name)

    # Patch cords - only required: name, core_diameter, numerical_aperture
    patch_cord_0 = devices.FiberPatchCord(
        name="Patch Cord 0",
        core_diameter=200,
        numerical_aperture=0.37,
    )

    patch_cord_1 = devices.FiberPatchCord(
        name="Patch Cord 1",
        core_diameter=200,
        numerical_aperture=0.37,
    )

    patch_cord_2 = devices.FiberPatchCord(
        name="Patch Cord 2",
        core_diameter=200,
        numerical_aperture=0.37,
    )

    patch_cord_3 = devices.FiberPatchCord(
        name="Patch Cord 3",
        core_diameter=200,
        numerical_aperture=0.37,
    )

    # LEDs - required: name, manufacturer, wavelength
    light_source_1 = devices.LightEmittingDiode(
        name="470nm LED",
        manufacturer=devices.Organization.THORLABS,
        wavelength=470,
    )

    light_source_2 = devices.LightEmittingDiode(
        name="415nm LED",
        manufacturer=devices.Organization.THORLABS,
        wavelength=415,
    )

    light_source_3 = devices.LightEmittingDiode(
        name="565nm LED",
        manufacturer=devices.Organization.THORLABS,
        wavelength=565,
    )

    # Detectors - required: name, manufacturer, detector_type, data_interface
    detector_1 = devices.Detector(
        name="Green CMOS",
        manufacturer=devices.Organization.FLIR,
        detector_type="Camera",
        data_interface="USB",
    )

    detector_2 = devices.Detector(
        name="Red CMOS",
        manufacturer=devices.Organization.FLIR,
        detector_type="Camera",
        data_interface="USB",
    )

    # Objective - required: name, numerical_aperture, magnification, immersion
    objective = devices.Objective(
        name="Objective",
        numerical_aperture=0.45,
        magnification=10,
        immersion="air",
    )

    # Filters - required: name, manufacturer, filter_type
    filter_1 = devices.Filter(
        name="Green emission filter",
        manufacturer=devices.Organization.SEMROCK,
        filter_type="Band pass",
    )

    filter_2 = devices.Filter(
        name="Red emission filter",
        manufacturer=devices.Organization.SEMROCK,
        filter_type="Band pass",
    )

    filter_3 = devices.Filter(
        name="Emission Dichroic",
        manufacturer=devices.Organization.SEMROCK,
        filter_type="Dichroic",
    )

    # Multiband filters require center_wavelength as a list
    filter_4 = devices.Filter(
        name="dual-edge standard epi-fluorescence dichroic beamsplitter",
        manufacturer=devices.Organization.SEMROCK,
        filter_type="Multiband",
        center_wavelength=[493, 574],
    )

    filter_5 = devices.Filter(
        name="Excitation filter 410nm",
        manufacturer=devices.Organization.THORLABS,
        filter_type="Band pass",
    )

    filter_6 = devices.Filter(
        name="Excitation filter 470nm",
        manufacturer=devices.Organization.THORLABS,
        filter_type="Band pass",
    )

    filter_7 = devices.Filter(
        name="Excitation filter 560nm",
        manufacturer=devices.Organization.THORLABS,
        filter_type="Band pass",
    )

    filter_8 = devices.Filter(
        name="450 Dichroic Longpass Filter",
        manufacturer=devices.Organization.EDMUND_OPTICS,
        filter_type="Dichroic",
    )

    filter_9 = devices.Filter(
        name="500 Dichroic Longpass Filter",
        manufacturer=devices.Organization.EDMUND_OPTICS,
        filter_type="Dichroic",
    )

    # Lens - required: name, manufacturer
    lens = devices.Lens(
        name="Image focusing lens",
        manufacturer=devices.Organization.THORLABS,
    )

    # HarpDevice (cuttlefish) - required: name, harp_device_type, is_clock_generator
    cuttlefish = devices.HarpDevice(
        name="cuTTLefishFip",
        harp_device_type=devices.HarpDeviceType.CUTTLEFISHFIP,
        is_clock_generator=False,
    )

    # HarpDevice (white rabbit) - required: name, harp_device_type, is_clock_generator
    # Note: channels needed for connection to reference them
    white_rabbit = devices.HarpDevice(
        name="harp_clock_generator",
        harp_device_type=devices.HarpDeviceType.WHITERABBIT,
        is_clock_generator=True,
        channels=[
            devices.DAQChannel(channel_name="ClkOut", channel_type=devices.DaqChannelType.DO),
        ],
    )

    # Connections - required: source_device, target_device
    # Ports are optional but needed if connections reference them
    connections = [
        Connection(
            source_device="cuTTLefishFip",
            target_device=computer_name,
        ),
        Connection(
            source_device="harp_clock_generator",
            source_port="ClkOut",
            target_device="cuTTLefishFip",
            target_port="COM1",
        ),
    ]

    instrument_model = instrument.Instrument(
        instrument_id=instrument_id,
        modification_date=date.today(),
        modalities=[Modality.FIB],
        coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
        location=location if location else None,
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
    base_path: Optional[str] = None,
    skip_confirmation: bool = False,
    input_func=input,
) -> None:
    """Create and save minimal instrument JSON.

    Parameters
    ----------
    instrument_id : Optional[str]
        Optional instrument ID. If None, will prompt user.
    base_path : Optional[str]
        Optional base path for instrument store.
    skip_confirmation : bool
        If True, skip confirmation prompt for new instrument IDs.
    input_func : callable
        Function to get user input. Defaults to builtin input().
    """
    # Get list of existing instrument IDs for help message
    existing_ids = list_instrument_ids(base_path=base_path)
    if existing_ids:
        help_message = "Below is a list of existing instrument IDs:\n" + "\n".join(f"  - {id}" for id in existing_ids)
    else:
        help_message = "No existing instrument IDs found in store."

    # Get instrument_id from parameter or prompt
    if instrument_id is None:
        instrument_id = prompt_for_string(
            "Instrument ID", required=True, help_message=help_message, input_func=input_func
        )

    # Load previous instrument data for defaults
    previous_instrument_dict = get_instrument(instrument_id, base_path=base_path)
    previous_instrument_obj = None
    if previous_instrument_dict is not None:
        previous_instrument_obj = instrument.Instrument.model_validate(previous_instrument_dict)

    # Get defaults from previous instrument
    default_computer_name, default_location = _get_defaults_from_previous_instrument(previous_instrument_obj)

    # Check if instrument exists, and if not, confirm creation of new ID
    if previous_instrument_dict is None:
        _confirm_new_instrument_id(instrument_id, existing_ids, skip_confirmation, input_func)

    # Prompt for location/room (optional, defaults to previous value)
    location = prompt_for_string("Location/Room", default=default_location, required=False, input_func=input_func)
    if not location:
        location = None

    # Prompt for computer name (optional, defaults to previous value or hostname)
    computer_name = prompt_for_string(
        "Computer name", default=default_computer_name, required=False, input_func=input_func
    )
    if not computer_name:
        computer_name = default_computer_name

    print("\nCreating minimal instrument:")
    print(f"  Instrument ID: {instrument_id}")
    print(f"  Location: {location if location else '(not set)'}")
    print(f"  Computer name: {computer_name}")

    instrument_model = create_minimal_instrument(instrument_id, computer_name, location)

    # Write to temporary file, then save to instrument store
    temp_dir = tempfile.mkdtemp()
    instrument_model.write_standard_file(Path(temp_dir))
    temp_path = Path(temp_dir) / instrument_model.default_filename()

    # Save to instrument store using the instrument_id we prompted for
    stored_path = save_instrument(path=str(temp_path), rig_id=instrument_id, base_path=base_path)
    print(f"Instrument saved to store: {stored_path}")

    # Also write to local JSON file for easy access
    output_path = Path(__file__).parent / "instrument_fip_minimal.json"
    instrument_model.write_standard_file(output_path.parent)
    output_file = output_path.parent / instrument_model.default_filename()

    # Rename to our desired filename
    if output_file != output_path:
        output_file.rename(output_path)

    print(f"Minimal instrument also saved to: {output_path}")
    print("\nNote: This is the absolute minimum required fields.")
    print("All optional fields (serial numbers, models, technical specs, etc.) have been removed.")

    # Clean up temporary file and directory
    temp_path.unlink()
    Path(temp_dir).rmdir()


if __name__ == "__main__":
    main()
