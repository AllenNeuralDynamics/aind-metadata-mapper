"""Tests for make_instrument module."""

import json
import socket

import pytest

import aind_metadata_mapper.instrument_store as instrument_store_module
from aind_metadata_mapper.fip.make_instrument import (
    create_instrument,
    extract_value,
    main,
    prompt_for_string,
    prompt_yes_no,
)


def test_extract_value_top_level():
    """Test extracting top-level field."""
    data = {"location": "428", "instrument_id": "test"}
    result = extract_value(data, "location")
    assert result == "428"


def test_extract_value_top_level_missing():
    """Test extracting missing top-level field."""
    data = {"instrument_id": "test"}
    result = extract_value(data, "location")
    assert result is None


def test_extract_value_component_by_name():
    """Test extracting component field by name."""
    data = {
        "components": [
            {"name": "Green CMOS", "serial_number": "12345"},
            {"name": "Red CMOS", "serial_number": "67890"},
        ]
    }
    result = extract_value(data, "Green CMOS", field="serial_number")
    assert result == "12345"


def test_extract_value_component_by_name_missing():
    """Test extracting from missing component."""
    data = {"components": [{"name": "Other", "serial_number": "12345"}]}
    result = extract_value(data, "Green CMOS", field="serial_number")
    assert result is None


def test_extract_value_component_by_class():
    """Test extracting component field by class name."""
    data = {
        "components": [
            {"__class_name": "Computer", "name": "test_computer"},
            {"__class_name": "Detector", "name": "detector1"},
        ]
    }
    result = extract_value(data, "Computer", field="name", component_class="Computer")
    assert result == "test_computer"


def test_extract_value_component_by_class_missing():
    """Test extracting from missing component class."""
    data = {"components": [{"__class_name": "Detector", "name": "detector1"}]}
    result = extract_value(data, "Computer", field="name", component_class="Computer")
    assert result is None


def test_extract_value_none_data():
    """Test extracting from None data."""
    result = extract_value(None, "location")
    assert result is None


def test_extract_value_field_required_for_component():
    """Test that field is required for component extraction."""
    data = {"components": [{"name": "test", "serial_number": "123"}]}
    # When extracting from components by class, field is required
    with pytest.raises(ValueError, match="field parameter is required"):
        extract_value(data, "Computer", component_class="Computer")


def test_create_instrument_with_values(tmp_path):
    """Test creating instrument with provided values in a temporary path."""
    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1_serial",
        "detector_2_serial": "det2_serial",
        "objective_serial": "obj_serial",
    }

    instrument = create_instrument(
        "test_rig",
        values=values,
        previous_instrument=None,
        base_path=str(tmp_path),
    )

    assert instrument.instrument_id == "test_rig"
    assert instrument.location == "428"
    assert len(instrument.components) > 0

    # Check that computer name was used
    computer = next((c for c in instrument.components if hasattr(c, "name") and c.name == "test_computer"), None)
    assert computer is not None

    # Check that serial numbers were used
    detector1 = next((c for c in instrument.components if hasattr(c, "name") and c.name == "Green CMOS"), None)
    assert detector1 is not None
    assert detector1.serial_number == "det1_serial"


def test_create_instrument_with_previous_instrument(tmp_path):
    """Test creating instrument with previous instrument data in a temporary path."""
    # Create a previous instrument in store
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()
    previous_file = rig_dir / "instrument.json"
    previous_data = {
        "instrument_id": "test_rig",
        "modification_date": "2025-01-15",
        "location": "428",
        "components": [
            {"__class_name": "Computer", "name": "previous_computer"},
            {"name": "Green CMOS", "serial_number": "prev_serial"},
        ],
    }
    with open(previous_file, "w", encoding="utf-8") as f:
        json.dump(previous_data, f)

    values = {
        "location": "429",  # Override location
        "computer_name": "new_computer",
        "detector_1_serial": "new_serial",
        "detector_2_serial": "det2_serial",
        "objective_serial": "obj_serial",
    }

    instrument = create_instrument(
        "test_rig",
        values=values,
        previous_instrument=None,  # Will load from store
        base_path=str(tmp_path),
    )

    assert instrument.location == "429"  # Uses provided value, not previous
    assert instrument.instrument_id == "test_rig"


def test_create_instrument_with_previous_instrument_dict():
    """Test creating instrument with previous_instrument dict provided."""
    previous_instrument = {
        "location": "428",
        "components": [
            {"__class_name": "Computer", "name": "prev_computer"},
            {"name": "Green CMOS", "serial_number": "prev_serial"},
        ],
    }

    values = {
        "location": "429",
        "computer_name": "new_computer",
        "detector_1_serial": "new_serial",
        "detector_2_serial": "det2_serial",
        "objective_serial": "obj_serial",
    }

    instrument = create_instrument(
        "test_rig",
        values=values,
        previous_instrument=previous_instrument,
    )

    assert instrument.location == "429"
    assert instrument.instrument_id == "test_rig"


def test_create_instrument_minimal_values():
    """Test creating instrument with minimal values."""
    values = {
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    instrument = create_instrument("test_rig", values=values)

    assert instrument.instrument_id == "test_rig"
    assert instrument.location is None  # Not provided, defaults to None
    # Computer name should default to system hostname
    assert len(instrument.components) > 0


def test_create_instrument_all_components_present():
    """Test that all expected components are created."""
    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    instrument = create_instrument("test_rig", values=values)

    component_names = [c.name for c in instrument.components if hasattr(c, "name")]

    # Check key components exist
    assert "test_computer" in component_names
    assert "Patch Cord 0" in component_names
    assert "Patch Cord 1" in component_names
    assert "Patch Cord 2" in component_names
    assert "Patch Cord 3" in component_names
    assert "Green CMOS" in component_names
    assert "Red CMOS" in component_names
    assert "Objective" in component_names
    assert "cuTTLefishFip" in component_names
    assert "Photometry Clock" in component_names


def test_create_instrument_connections():
    """Test that connections are created correctly."""
    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    instrument = create_instrument("test_rig", values=values)

    assert len(instrument.connections) == 2
    connection_targets = [c.target_device for c in instrument.connections]
    assert "cuTTLefishFip" in connection_targets
    assert "test_computer" in connection_targets


def test_extract_value_component_missing_field():
    """Test extracting field that doesn't exist in component."""
    data = {
        "components": [
            {"name": "Green CMOS", "serial_number": "12345"},
        ]
    }
    result = extract_value(data, "Green CMOS", field="nonexistent_field")
    assert result is None


def test_extract_value_empty_components():
    """Test extracting from instrument with empty components list."""
    data = {"components": []}
    result = extract_value(data, "Green CMOS", field="serial_number")
    assert result is None


def test_extract_value_no_components_key():
    """Test extracting from instrument without components key."""
    data = {"location": "428"}
    result = extract_value(data, "Green CMOS", field="serial_number")
    assert result is None


def test_create_instrument_empty_location():
    """Test creating instrument with empty location."""
    values = {
        "location": "",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    instrument = create_instrument("test_rig", values=values)
    assert instrument.location is None


def test_create_instrument_default_computer_name():
    """Test that computer name defaults to hostname when not provided."""
    values = {
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    instrument = create_instrument("test_rig", values=values)

    # Computer name should default to system hostname
    computer = next(
        (c for c in instrument.components if hasattr(c, "name") and c.name == socket.gethostname()),
        None,
    )
    # Verify computer was created with hostname
    assert computer is not None
    assert instrument.instrument_id == "test_rig"


# CLI tests (main function)


def test_main_with_provided_values(tmp_path):
    """Test main() function with provided values in a temporary path."""
    # Reset the global store to ensure we use the provided base_path
    instrument_store_module._default_store = None

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    main(
        instrument_id="test_rig",
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=True,
    )

    # Verify instrument was saved
    saved_file = tmp_path / "test_rig" / "instrument.json"
    assert saved_file.exists()

    # Verify content
    with open(saved_file, "r", encoding="utf-8") as f:
        saved_data = json.load(f)
    assert saved_data["instrument_id"] == "test_rig"
    assert saved_data["location"] == "428"


def test_main_with_existing_instrument(tmp_path):
    """Test main() with existing instrument in store in a temporary path."""
    # Reset the global store to ensure we use the provided base_path
    instrument_store_module._default_store = None

    # Create existing instrument
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()
    existing_file = rig_dir / "instrument.json"
    existing_data = {
        "instrument_id": "test_rig",
        "modification_date": "2025-01-15",
        "location": "428",
    }
    with open(existing_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f)

    values = {
        "location": "429",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    main(
        instrument_id="test_rig",
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=True,
    )

    # Verify archive was created
    archive_files = list(rig_dir.glob("instrument_*.json"))
    assert len(archive_files) == 1

    # Verify current file was updated
    with open(existing_file, "r", encoding="utf-8") as f:
        current_data = json.load(f)
    assert current_data["location"] == "429"


def test_main_new_instrument_creates_directory(tmp_path):
    """Test that main() creates directory for new instrument in a temporary path."""
    # Reset the global store to ensure we use the provided base_path
    instrument_store_module._default_store = None

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    main(
        instrument_id="new_rig",
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=True,
    )

    # Verify directory and file were created
    rig_dir = tmp_path / "new_rig"
    assert rig_dir.exists()
    assert (rig_dir / "instrument.json").exists()


def test_main_with_existing_ids_help_message(tmp_path):
    """Test main() generates help message when existing IDs are found in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    # Create existing instruments
    for rig_id in ["rig1", "rig2"]:
        rig_dir = tmp_path / rig_id
        rig_dir.mkdir()
        (rig_dir / "instrument.json").touch()

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    # This should work and generate help message (but we skip confirmation)
    main(
        instrument_id="new_rig",
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=True,
    )

    # Verify it was created
    assert (tmp_path / "new_rig" / "instrument.json").exists()


def test_main_with_no_existing_ids_help_message(tmp_path):
    """Test main() generates help message when no existing IDs are found in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    # This should work with empty help message
    main(
        instrument_id="new_rig",
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=True,
    )

    # Verify it was created
    assert (tmp_path / "new_rig" / "instrument.json").exists()


# Prompt function tests


def test_prompt_yes_no_with_yes():
    """Test prompt_yes_no returns True for 'yes' input."""

    def test_input(prompt):
        """Test input function that returns 'yes'."""
        return "yes"

    result = prompt_yes_no("Continue?", input_func=test_input)
    assert result is True


def test_prompt_yes_no_with_y():
    """Test prompt_yes_no returns True for 'y' input."""

    def test_input(prompt):
        """Test input function that returns 'y'."""
        return "y"

    result = prompt_yes_no("Continue?", input_func=test_input)
    assert result is True


def test_prompt_yes_no_with_no():
    """Test prompt_yes_no returns False for 'no' input."""

    def test_input(prompt):
        """Test input function that returns 'no'."""
        return "no"

    result = prompt_yes_no("Continue?", input_func=test_input)
    assert result is False


def test_prompt_yes_no_with_empty_default_true():
    """Test prompt_yes_no uses default=True when input is empty."""

    def test_input(prompt):
        """Test input function that returns empty string."""
        return ""

    result = prompt_yes_no("Continue?", default=True, input_func=test_input)
    assert result is True


def test_prompt_yes_no_with_empty_default_false():
    """Test prompt_yes_no uses default=False when input is empty."""

    def test_input(prompt):
        """Test input function that returns empty string."""
        return ""

    result = prompt_yes_no("Continue?", default=False, input_func=test_input)
    assert result is False


def test_prompt_yes_no_case_insensitive():
    """Test prompt_yes_no is case insensitive."""

    def test_input(prompt):
        """Test input function that returns 'YES' (uppercase)."""
        return "YES"

    result = prompt_yes_no("Continue?", input_func=test_input)
    assert result is True


def test_prompt_for_string_with_input():
    """Test prompt_for_string returns user input."""

    def test_input(prompt):
        """Test input function that returns 'test_value'."""
        return "test_value"

    result = prompt_for_string("Enter value:", input_func=test_input)
    assert result == "test_value"


def test_prompt_for_string_with_default():
    """Test prompt_for_string returns default when input is empty."""

    def test_input(prompt):
        """Test input function that returns empty string."""
        return ""

    result = prompt_for_string("Enter value:", default="default_value", input_func=test_input)
    assert result == "default_value"


def test_prompt_for_string_with_empty_not_required():
    """Test prompt_for_string returns empty string when not required and no default."""

    def test_input(prompt):
        """Test input function that returns empty string."""
        return ""

    result = prompt_for_string("Enter value:", required=False, input_func=test_input)
    assert result == ""


def test_prompt_for_string_required_with_help():
    """Test prompt_for_string shows help message when required field is empty."""
    help_printed = []

    def test_input(prompt):
        """Test input function that returns empty first, then 'final_value'."""
        if not help_printed:
            help_printed.append(True)
            return ""  # First call returns empty
        return "final_value"  # Second call returns value

    result = prompt_for_string("Enter value:", required=True, help_message="Help text", input_func=test_input)
    assert result == "final_value"
    assert len(help_printed) == 1  # Help was shown


def test_prompt_for_string_strips_whitespace():
    """Test prompt_for_string strips whitespace from input."""

    def test_input(prompt):
        """Test input function that returns value with whitespace."""
        return "  test_value  "

    result = prompt_for_string("Enter value:", input_func=test_input)
    assert result == "test_value"


def test_create_instrument_with_prompts(tmp_path):
    """Test create_instrument prompts for values when values dict is not provided in a temporary path."""
    call_count = 0

    def test_input(prompt):
        """Test input function that returns values in sequence."""
        nonlocal call_count
        call_count += 1
        # Return values in order: location, computer_name, detector_1, detector_2, objective
        responses = ["428", "test_computer", "det1", "det2", "obj"]
        return responses[call_count - 1]

    instrument = create_instrument(
        "test_rig",
        values=None,  # Will prompt
        previous_instrument=None,
        base_path=str(tmp_path),
        input_func=test_input,
    )

    assert instrument.instrument_id == "test_rig"
    assert instrument.location == "428"
    assert call_count == 5  # Should have prompted 5 times


def test_main_prompts_for_instrument_id(tmp_path):
    """Test main() prompts for instrument_id when not provided in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    call_count = 0

    def test_input(prompt):
        """Test input function that returns 'test_rig' for Instrument ID prompt."""
        nonlocal call_count
        call_count += 1
        if "Instrument ID" in prompt:
            return "test_rig"
        return ""

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    main(
        instrument_id=None,  # Will prompt
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=True,
        input_func=test_input,
    )

    assert (tmp_path / "test_rig" / "instrument.json").exists()
    assert call_count >= 1  # Should have prompted for instrument_id


def test_main_prompts_for_confirmation(tmp_path):
    """Test main() prompts for confirmation when creating new instrument in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    confirmation_called = False

    def test_input(prompt):
        """Test input function that returns 'yes' for confirmation prompt."""
        nonlocal confirmation_called
        if "Are you sure" in prompt:
            confirmation_called = True
            return "yes"
        return ""

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    main(
        instrument_id="new_rig",
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=False,  # Will prompt for confirmation
        input_func=test_input,
    )

    assert confirmation_called
    assert (tmp_path / "new_rig" / "instrument.json").exists()


def test_main_confirmation_with_existing_ids(tmp_path):
    """Test main() shows existing IDs list when prompting for confirmation in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    # Create existing instruments
    for rig_id in ["rig1", "rig2"]:
        rig_dir = tmp_path / rig_id
        rig_dir.mkdir()
        (rig_dir / "instrument.json").touch()

    confirmation_called = False

    def test_input(prompt):
        """Test input function that returns 'yes' for confirmation prompt."""
        nonlocal confirmation_called
        if "Are you sure" in prompt:
            confirmation_called = True
            return "yes"
        return ""

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    main(
        instrument_id="new_rig",
        values=values,
        base_path=str(tmp_path),
        skip_confirmation=False,
        input_func=test_input,
    )

    assert confirmation_called
    assert (tmp_path / "new_rig" / "instrument.json").exists()


def test_main_confirmation_cancelled(tmp_path):
    """Test main() exits when user cancels confirmation in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    def test_input(prompt):
        """Test input function that returns 'no' for confirmation prompt."""
        if "Are you sure" in prompt:
            return "no"  # User cancels
        return ""

    values = {
        "location": "428",
        "computer_name": "test_computer",
        "detector_1_serial": "det1",
        "detector_2_serial": "det2",
        "objective_serial": "obj",
    }

    # Should exit with sys.exit(0) when cancelled
    with pytest.raises(SystemExit) as exc_info:
        main(
            instrument_id="new_rig",
            values=values,
            base_path=str(tmp_path),
            skip_confirmation=False,
            input_func=test_input,
        )

    assert exc_info.value.code == 0
    # Instrument should not be created
    assert not (tmp_path / "new_rig" / "instrument.json").exists()
