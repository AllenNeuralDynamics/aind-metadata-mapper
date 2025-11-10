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
        2. Load previous instrument data for that ID if available
        3. Prompt for computer name (defaults to system hostname or previous value)
        4. Prompt for serial numbers (defaults to previous values if available)
        5. Save the instrument to /allen/aind/scratch/instrument_store/{instrument_id}/

Output:
    - /allen/aind/scratch/instrument_store/{instrument_id}/instrument.json: Current instrument saved to instrument store
    - /allen/aind/scratch/instrument_store/{instrument_id}/instrument_YYYYMMDD.json: Previous versions archived by date
"""

import socket
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import aind_data_schema.components.devices as d
import aind_data_schema.core.instrument as r
from aind_data_schema.components.connections import Connection
from aind_data_schema.components.coordinates import CoordinateSystemLibrary
from aind_data_schema.components.devices import Computer
from aind_data_schema.components.identifiers import Software
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.instrument_store import get_instrument, list_instrument_ids, save_instrument


def prompt_yes_no(prompt: str, default: bool = True) -> bool:
    """Prompt user for yes/no response.

    Parameters
    ----------
    prompt : str
        The prompt message.
    default : bool
        Default value if user presses Enter. Defaults to True (yes).

    Returns
    -------
    bool
        True for yes, False for no.
    """
    default_str = "Y/n" if default else "y/N"
    full_prompt = f"{prompt} [{default_str}]: "
    response = input(full_prompt).strip().lower()

    if not response:
        return default
    return response in ("y", "yes")


def prompt_for_string(
    prompt: str, default: Optional[str] = None, required: bool = False, help_message: Optional[str] = None
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
        if help_message:
            print(help_message)


def extract_value(
    instrument_data: dict,
    source: str,
    field: Optional[str] = None,
    component_class: Optional[str] = None,
) -> Optional[str]:
    """Extract a value from instrument data.

    Can extract:
    - Top-level fields (e.g., source="location", field=None, component_class=None)
    - Component fields by component name (e.g., source="Green CMOS", field="serial_number")
    - Component fields by class name (e.g., source="Computer", field="name", component_class="Computer")

    Parameters
    ----------
    instrument_data : dict
        Instrument data loaded from JSON.
    source : str
        For top-level fields: the field name (e.g., "location").
        For components: the component name or class name to find.
    field : Optional[str]
        Field name to extract. Required for component extraction, None for top-level fields.
    component_class : Optional[str]
        If provided, searches for component by class name instead of component name.

    Returns
    -------
    Optional[str]
        Extracted value if found, None otherwise.
    """
    if instrument_data is None:
        return None

    # Handle top-level fields
    if component_class is None and field is None:
        return instrument_data.get(source)

    # Handle component extraction (field is required)
    if field is None:
        raise ValueError("field parameter is required when extracting from components")

    components = instrument_data.get("components", [])
    for component in components:
        # Match by class name if provided
        if component_class:
            if component.get("__class_name") == component_class:
                return component.get(field)
        # Match by component name
        elif component.get("name") == source:
            return component.get(field)

    return None


def create_instrument(instrument_id: str) -> r.Instrument:
    """Create an FIP instrument interactively.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier for loading previous instrument defaults.

    Returns
    -------
    r.Instrument
        Created instrument object.
    """
    # Try to load previous instrument for this ID
    previous_instrument = get_instrument(instrument_id)

    # Prompt for location with previous value as default
    location = prompt_for_string("Location", extract_value(previous_instrument, "location"))

    # Prompt for computer name with system default or previous value
    system_hostname = socket.gethostname()
    previous_computer_name = extract_value(previous_instrument, "Computer", field="name", component_class="Computer")
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
    detector_1_serial_default = extract_value(previous_instrument, "Green CMOS", field="serial_number")
    detector_2_serial_default = extract_value(previous_instrument, "Red CMOS", field="serial_number")
    objective_serial_default = extract_value(previous_instrument, "Objective", field="serial_number")

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
        detector_type="CMOS",
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
        detector_type="CMOS",
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

    cuttlefish = d.Device(name="cuTTLefishFip")

    connections = [
        Connection(
            source_device="Photometry Clock",
            target_device="cuTTLefishFip",
        ),
        Connection(
            source_device="cuTTLefishFip",
            target_device=computer_name,
        ),
    ]

    photemetry_clock = d.Device(name="Photometry Clock")

    instrument = r.Instrument(
        location=location if location else None,
        instrument_id=instrument_id,
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
            cuttlefish,
            photemetry_clock,
        ],
        calibrations=[],
        connections=connections,
    )

    return instrument


def main() -> None:
    """Main function to create an instrument interactively."""
    # Get list of existing instrument IDs for help message
    existing_ids = list_instrument_ids()
    if existing_ids:
        help_message = "Below is a list of existing instrument IDs:\n" + "\n".join(f"  - {id}" for id in existing_ids)
    else:
        help_message = "No existing instrument IDs found in store."

    # Prompt for instrument_id (required, no default)
    instrument_id = prompt_for_string("Instrument ID", required=True, help_message=help_message)

    # Check if instrument exists, and if not, confirm creation of new ID
    previous_instrument = get_instrument(instrument_id)
    if previous_instrument is None:
        print("This is a new instrument ID.")
        if existing_ids:
            print("Here is a list of existing instrument IDs:")
            for id in existing_ids:
                print(f"  - {id}")
        else:
            print("No existing instrument IDs found in store.")
        print()
        if not prompt_yes_no(f"Are you sure you want to create a new ID with name '{instrument_id}'?", default=True):
            print("Cancelled. Please run the script again with the correct instrument ID.")
            sys.exit(0)

    # Create instrument interactively
    instrument = create_instrument(instrument_id)

    # Write to temporary file, then save to instrument store
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        f.write(instrument.model_dump_json(indent=2))
        temp_path = f.name

    # Save to instrument store using the instrument_id we prompted for
    stored_path = save_instrument(path=temp_path, rig_id=instrument_id)
    print(f"Instrument saved to store: {stored_path}")

    # Clean up temporary file
    Path(temp_path).unlink()


if __name__ == "__main__":
    main()
