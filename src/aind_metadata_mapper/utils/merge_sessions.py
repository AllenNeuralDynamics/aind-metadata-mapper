"""Utility functions for merging multiple session metadata files."""

import json
import argparse
from pathlib import Path
from typing import Any, Dict, List, Union
import logging
from datetime import datetime

# Configure logging at INFO level with timestamp
logging.basicConfig(
    level=logging.INFO,
    format="\n%(asctime)s - %(message)s\n",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _merge_lists(list1: List[Any], list2: List[Any]) -> List[Any]:
    """Merge two lists, removing duplicates while preserving order.

    For lists of dictionaries, merges based on content rather than identity.
    For simple types (str, int, etc), removes duplicates using dict.fromkeys.
    """
    # If lists contain dictionaries, append all items
    if any(isinstance(item, dict) for item in list1 + list2):
        return list1 + list2

    # For simple types, deduplicate while preserving order
    return list(dict.fromkeys(list1 + list2))


def _prompt_for_field(
    field_name: str, value1: str, value2: str, file1: str, file2: str
) -> str:
    """Prompt user to resolve conflicting string fields.

    Args:
        field_name: Name of the field with conflict
        value1: Value from first file
        value2: Value from second file
        file1: Name of first file
        file2: Name of second file

    Returns:
        str: User's chosen value for the field
    """
    default = f"{value1} + {value2}"
    logging.info(
        f"Conflict in '{field_name}':\n"
        f"  {file1}: {value1}\n"
        f"  {file2}: {value2}\n"
        f"Default merged value: {default}"
    )
    response = input("Accept default? [Y/n] or type new value: ").strip()

    if not response or response.lower() == "y":
        return default
    return response


def _format_time_difference(diff_seconds: float) -> str:
    """Format a time difference in seconds into a human readable string.

    Args:
        diff_seconds: Time difference in seconds

    Returns:
        Human readable string describing the time
        difference with appropriate units
        and precision based on the magnitude of the difference.
    """
    if diff_seconds >= 3600:  # More than an hour
        hours = int(diff_seconds // 3600)
        minutes = int((diff_seconds % 3600) // 60)
        diff_str = f"{hours} hour{'s' if hours != 1 else ''}"
        if minutes > 0:
            diff_str += f", {minutes} minute{'s' if minutes != 1 else ''}"
    elif diff_seconds >= 60:  # More than a minute
        minutes = int(diff_seconds // 60)
        seconds = diff_seconds % 60
        diff_str = f"{minutes} minute{'s' if minutes != 1 else ''}"
        if seconds > 0:
            if seconds.is_integer():
                diff_str += (
                    f", {int(seconds)} second{'s' if seconds != 1 else ''}"
                )
            else:
                diff_str += f", {seconds:.3f} seconds"
    elif diff_seconds >= 1:  # More than a second
        if diff_seconds.is_integer():
            diff_str = (
                f"{int(diff_seconds)} second{'s' if diff_seconds != 1 else ''}"
            )
        else:
            diff_str = f"{diff_seconds:.3f} seconds"
    else:  # Less than a second
        diff_str = f"{diff_seconds * 1000:.3f} milliseconds"

    return diff_str


def _merge_timestamps(
    field: str,
    time1: str,
    time2: str,
    tolerance_hours: float = 1.0,
    file1: str | None = None,
    file2: str | None = None,
) -> str:
    """Merge two ISO format UTC timestamps based on field name.

    For start times (containing 'start'), takes earlier timestamp.
    For end times (containing 'end'), takes later timestamp.
    Raises error if timestamps differ by more than tolerance.

    Args:
        field: Name of field being merged (determines earlier/later logic)
        time1: First timestamp in ISO format with Z suffix
        time2: Second timestamp in ISO format with Z suffix
        tolerance_hours: Maximum allowed difference in hours (default 1)
        file1: Optional name of first file for logging clarity
        file2: Optional name of second file for logging clarity

    Returns:
        Selected timestamp in ISO format

    Raises:
        ValueError: If timestamps differ by more than tolerance
    """
    t1 = datetime.fromisoformat(time1.replace("Z", "+00:00"))
    t2 = datetime.fromisoformat(time2.replace("Z", "+00:00"))

    # Calculate time difference in seconds
    diff_seconds = abs((t1 - t2).total_seconds())
    diff_str = _format_time_difference(diff_seconds)

    if diff_seconds / 3600 > tolerance_hours:
        raise ValueError(
            f"Timestamps differ by {diff_str}, "
            f"exceeding tolerance of {tolerance_hours} hours"
        )

    # Format the timestamp descriptions with file names if available
    time1_desc = f"{time1} (from {file1})" if file1 else time1
    time2_desc = f"{time2} (from {file2})" if file2 else time2

    if "start" in field.lower():
        result = min(time1, time2)
        result_source = file1 if result == time1 else file2
        result_desc = (
            f"{result} (from {result_source})" if result_source else result
        )
        logging.info(
            f"Two timestamps found for {field}:\n"
            f"  1: {time1_desc}\n"
            f"  2: {time2_desc}\n"
            f"These differ by {diff_str}.\n"
            f"Using earlier timestamp: {result_desc}"
        )
        return result
    elif "end" in field.lower():
        result = max(time1, time2)
        result_source = file1 if result == time1 else file2
        result_desc = (
            f"{result} (from {result_source})" if result_source else result
        )
        logging.info(
            f"Two timestamps found for {field}:\n"
            f"  1: {time1_desc}\n"
            f"  2: {time2_desc}\n"
            f"These differ by {diff_str}.\n"
            f"Using later timestamp: {result_desc}"
        )
        return result
    else:
        result = time1
        result_desc = f"{result} (from {file1})" if file1 else result
        logging.info(
            f"Two timestamps found for {field}:\n"
            f"  1: {time1_desc}\n"
            f"  2: {time2_desc}\n"
            f"These differ by {diff_str}.\n"
            f"Field name does not indicate start/end, "
            f"using first timestamp: {result_desc}"
        )
        return result


def _merge_values(
    field: str, val1: Any, val2: Any, file1: str, file2: str
) -> Any:
    """Merge two values based on their types."""
    if _is_none(val1, val2):
        return _merge_none(val1, val2)
    if _is_identical(val1, val2):
        return val1
    if _is_timestamp(field, val1, val2):
        return _merge_timestamp(field, val1, val2, file1, file2)
    if isinstance(val1, list) and isinstance(val2, list):
        return _merge_lists(val1, val2)
    if isinstance(val1, dict) and isinstance(val2, dict):
        return _merge_dicts(val1, val2, file1, file2)
    if isinstance(val1, str) and isinstance(val2, str):
        return _merge_strings(field, val1, val2, file1, file2)
    return _prompt_for_field(field, str(val1), str(val2), file1, file2)


def _is_none(val1, val2):
    return val1 is None or val2 is None


def _merge_none(val1, val2):
    return val2 if val1 is None else val1


def _is_identical(val1, val2):
    return val1 == val2


def _is_timestamp(field, val1, val2):
    return (
        isinstance(val1, str)
        and isinstance(val2, str)
        and "time" in field.lower()
        and all(t.endswith("Z") for t in [val1, val2])
    )


def _merge_timestamp(field, val1, val2, file1, file2):
    try:
        return _merge_timestamps(field, val1, val2, file1=file1, file2=file2)
    except ValueError:
        return _prompt_for_field(field, val1, val2, file1, file2)


def _merge_strings(field, val1, val2, file1, file2):
    # Special case: if one string is empty, use the non-empty one
    if val1 == "" and val2 != "":
        return val2
    if val2 == "" and val1 != "":
        return val1
    # If both are non-empty and different, prompt
    return _prompt_for_field(field, val1, val2, file1, file2)


def _merge_dicts(
    dict1: Dict[str, Any], dict2: Dict[str, Any], file1: str, file2: str
) -> Dict[str, Any]:
    """Recursively merge two dictionaries.

    Args:
        dict1: First dictionary
        dict2: Second dictionary
        file1: Name of first file
        file2: Name of second file

    Returns:
        Merged dictionary
    """
    merged = {}
    all_keys = set(dict1.keys()) | set(dict2.keys())

    for key in all_keys:
        if key in dict1 and key in dict2:
            # Special case for reward_consumed_unit: pass parent dicts
            if key == "reward_consumed_unit":
                merged[key] = _merge_reward_unit(dict1, dict2, file1, file2)
            else:
                merged[key] = _merge_values(
                    key, dict1[key], dict2[key], file1, file2
                )
        else:
            merged[key] = dict1.get(key) or dict2.get(key)

    return merged


def _merge_reward_unit(
    dict1: Dict[str, Any], dict2: Dict[str, Any], file1: str, file2: str
) -> str:
    """
    Special-case merge for reward_consumed_unit.
    Ignores the unit if the total is None.
    """
    total1 = dict1.get("reward_consumed_total")
    total2 = dict2.get("reward_consumed_total")
    unit1 = dict1.get("reward_consumed_unit")
    unit2 = dict2.get("reward_consumed_unit")

    # If one total is None, use the other's unit
    if total1 is None and total2 is not None:
        return unit2
    if total2 is None and total1 is not None:
        return unit1
    # If both totals are None, prefer non-null unit or default to unit1
    if total1 is None and total2 is None:
        return unit1 or unit2
    # If both totals are real, fall back to normal merge (prompt)
    return _merge_values("reward_consumed_unit", unit1, unit2, file1, file2)


def merge_sessions(
    session_file1: Union[str, Path],
    session_file2: Union[str, Path],
    output_file: Union[str, Path],
) -> Dict[str, Any]:
    """Merge two session metadata files into a single session.

    Args:
        session_file1: Path to first session JSON file
        session_file2: Path to second session JSON file
        output_file: Path where merged session JSON will be saved

    Returns:
        Dict containing merged session metadata

    Raises:
        ValueError: If files cannot be read or merged
    """
    try:
        with open(session_file1, "r") as f1, open(session_file2, "r") as f2:
            session1 = json.load(f1)
            session2 = json.load(f2)
    except Exception as e:
        raise ValueError(f"Error reading session files: {str(e)}")

    merged = _merge_dicts(
        session1, session2, Path(session_file1).name, Path(session_file2).name
    )

    # Write merged session to output file
    try:
        with open(output_file, "w") as f:
            json.dump(merged, f, indent=2)
    except Exception as e:
        raise ValueError(f"Error writing merged session file: {str(e)}")

    return merged


def main():
    """Command line interface for merging session files."""
    parser = argparse.ArgumentParser(
        description="Merge two session metadata JSON files into a single file"
    )
    parser.add_argument(
        "--file1",
        type=str,
        required=True,
        help="Path to first session JSON file",
    )
    parser.add_argument(
        "--file2",
        type=str,
        required=True,
        help="Path to second session JSON file",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Path to save merged session JSON file",
    )

    args = parser.parse_args()

    merge_sessions(args.file1, args.file2, args.output)

    logging.info(f"Merged session saved to: {args.output}")


if __name__ == "__main__":
    main()
