"""Utility functions for merging multiple session metadata files.

This module provides functions to merge two session JSON files, handling
special cases for certain fields and resolving conflicts interactively
when necessary.

Note that this module is designed for merging session.jsons,
which will no longer be generated by aind-data-schema >= 2.0
So this module will likely be deprecated and removed in the future.
"""

import json
import argparse
from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Tuple
import logging
from datetime import datetime

# Configure logging at INFO level with timestamp
logging.basicConfig(
    level=logging.INFO,
    format="\n%(asctime)s - %(message)s\n",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """Convert any ISO format timestamp string to a datetime object.

    Handles various ISO 8601 formats including Z suffix and timezone offsets.

    Parameters
    ----------
    timestamp_str : Optional[str]
        ISO format timestamp string, or None

    Returns
    -------
    Optional[datetime]
        Parsed datetime object with timezone information,
        or None if input was None

    Raises
    ------
    ValueError
        If the timestamp string cannot be parsed
    """
    if not timestamp_str:
        return None

    # Handle Z suffix by replacing with +00:00 for standard parsing
    if timestamp_str.endswith("Z"):
        timestamp_str = timestamp_str.replace("Z", "+00:00")

    # Parse the ISO format string
    return datetime.fromisoformat(timestamp_str)


def _merge_lists(list1: List[Any], list2: List[Any]) -> List[Any]:
    """Merge two lists, removing duplicates while preserving order."""
    # If lists contain dictionaries, append all items
    if any(isinstance(item, dict) for item in list1 + list2):
        return list1 + list2

    # For simple types, deduplicate while preserving order
    return list(dict.fromkeys(list1 + list2))


def _prompt_for_field(
    field_name: str, value1: str, value2: str, file1: str, file2: str
) -> str:
    """
    Prompt user to resolve conflicting string fields.

    Parameters
    ----------
    field_name : str
        Name of the field with conflict.
    value1 : str
        Value from first file.
    value2 : str
        Value from second file.
    file1 : str
        Name of first file.
    file2 : str
        Name of second file.

    Returns
    -------
    str
        User's chosen value for the field.
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
    file1: str | None = None,
    file2: str | None = None,
) -> str:
    """Merge two ISO format timestamps based on field name.

    For start times (containing 'start'), takes earlier timestamp.
    For end times (containing 'end'), takes later timestamp.
    Raises error if timestamps differ by more than tolerance.

    Args:
        field: Name of field being merged (determines earlier/later logic)
        time1: First timestamp in ISO format (with any timezone information)
        time2: Second timestamp in ISO format (with any timezone information)
        file1: Optional name of first file for logging clarity
        file2: Optional name of second file for logging clarity

    Returns:
        Selected timestamp in ISO format, preserving the original format

    Raises:
        ValueError: If timestamps differ by more than tolerance
    """
    # Parse timestamps to datetime objects
    try:
        t1 = _parse_timestamp(time1)
        t2 = _parse_timestamp(time2)

        if not t1 or not t2:
            raise ValueError("One or both timestamps are None or empty")
    except ValueError as e:
        raise ValueError(f"Failed to parse timestamp: {str(e)}")

    # Calculate time difference in seconds
    diff_seconds = abs((t1 - t2).total_seconds())
    diff_str = _format_time_difference(diff_seconds)

    # Format the timestamp descriptions with file names if available
    time1_desc = f"{time1} (from {file1})" if file1 else time1
    time2_desc = f"{time2} (from {file2})" if file2 else time2

    # Compare the datetime objects but return the original string format
    if "start" in field.lower():
        result = time1 if t1 <= t2 else time2
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
        result = time1 if t1 >= t2 else time2
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
    """
    Merge two values based on their types and field name.

    Handles special cases for timestamps, lists, dicts, and strings.
    For strings, if one is empty, returns the non-empty string.
    Prompts the user if both are non-empty and different.

    Parameters
    ----------
    field : str
        Name of the field being merged.
    val1 : Any
        Value from the first file.
    val2 : Any
        Value from the second file.
    file1 : str
        Name of the first file.
    file2 : str
        Name of the second file.

    Returns
    -------
    Any
        The merged value.
    """
    # Handle case where one value is None
    if val1 is None:
        return val2
    if val2 is None:
        return val1

    # If values are identical, return either
    if val1 == val2:
        return val1

    # Handle timestamps specially
    if (
        isinstance(val1, str)
        and isinstance(val2, str)
        and "time" in field.lower()
        and (
            val1.find("T") > 0 or val2.find("T") > 0
        )  # ISO datetime format check
    ):

        return _merge_timestamps(
            field=field,
            time1=val1,
            time2=val2,
            file1=file1,
            file2=file2,
        )

    # Handle other types
    if isinstance(val1, list) and isinstance(val2, list):
        return _merge_lists(val1, val2)
    if isinstance(val1, dict) and isinstance(val2, dict):
        return _merge_dicts(val1, val2, file1, file2)
    if isinstance(val1, str) and isinstance(val2, str):
        return _merge_strings(field, val1, val2, file1, file2)
    return _prompt_for_field(field, str(val1), str(val2), file1, file2)


def _merge_strings(field, val1, val2, file1, file2):
    """
    Merge two strings, preferring non-empty or
    prompting if both non-empty and different.
    """
    if val1 == "" and val2 != "":
        return val2
    if val2 == "" and val1 != "":
        return val1
    # If both are non-empty and different, prompt
    return _prompt_for_field(field, val1, val2, file1, file2)


def _should_merge_streams(
    stream1: Dict[str, Any],
    stream2: Dict[str, Any],
    tolerance_minutes: float = 5.0,
) -> bool:
    """Check if two streams should be merged based on their timing."""
    try:
        # Parse the start times
        start1_dt = _parse_timestamp(stream1.get("stream_start_time"))
        start2_dt = _parse_timestamp(stream2.get("stream_start_time"))

        if not start1_dt or not start2_dt:
            return False

        # Parse the end times
        end1_dt = _parse_timestamp(stream1.get("stream_end_time"))
        end2_dt = _parse_timestamp(stream2.get("stream_end_time"))

        if not end1_dt or not end2_dt:
            return False

        # Calculate time differences
        start_diff = abs((start1_dt - start2_dt).total_seconds())
        end_diff = abs((end1_dt - end2_dt).total_seconds())

        # Check if streams should be merged (comparing in minutes)
        return (
            start_diff / 60 <= tolerance_minutes
            and end_diff / 60 <= tolerance_minutes
        )

    except (ValueError, AttributeError):
        # If there's any error parsing dates, don't merge
        return False


def _merge_two_streams(
    stream1: Dict[str, Any], stream2: Dict[str, Any], file1: str, file2: str
) -> Dict[str, Any]:
    """Merge two individual streams into one."""
    merged = {}
    all_keys = set(stream1.keys()) | set(stream2.keys())

    for key in all_keys:
        if key in stream1 and key in stream2:
            val1 = stream1[key]
            val2 = stream2[key]

            # For timestamps, use standard merge logic
            if (
                key in ["stream_start_time", "stream_end_time"]
                and val1
                and val2
            ):
                try:
                    merged[key] = _merge_timestamps(
                        field=key,
                        time1=val1,
                        time2=val2,
                        file1=file1,
                        file2=file2,
                    )
                except ValueError:
                    # If merge fails, use first value
                    merged[key] = val1
            # For lists, simply combine them
            elif isinstance(val1, list) and isinstance(val2, list):
                merged[key] = _merge_lists(val1, val2)
            # For other types, use standard value merging
            else:
                merged[key] = _merge_values(key, val1, val2, file1, file2)
        else:
            # Key only in one stream
            merged[key] = (
                stream1.get(key) if key in stream1 else stream2.get(key)
            )

    return merged


def _find_mergeable_pair(
    streams: List[Dict[str, Any]],
) -> Optional[Tuple[int, int]]:
    """Find the first pair of streams that can be merged."""
    for i in range(len(streams)):
        for j in range(i + 1, len(streams)):
            if _should_merge_streams(streams[i], streams[j]):
                return (i, j)
    return None


def _merge_data_streams(
    streams: List[Dict[str, Any]], file1: str, file2: str
) -> List[Dict[str, Any]]:
    """
    Recursively merge data streams with similar start and end times.

    Parameters
    ----------
    streams : List[Dict[str, Any]]
        List of data stream dictionaries to merge
    file1 : str
        Name of the first file
    file2 : str
        Name of the second file

    Returns
    -------
    List[Dict[str, Any]]
        List of merged data streams
    """
    # Base case: 0 or 1 stream, nothing to merge
    if len(streams) <= 1:
        return streams

    # Find the first pair of streams that can be merged
    mergeable_pair = _find_mergeable_pair(streams)
    if mergeable_pair is None:
        # No more streams can be merged
        return streams

    i, j = mergeable_pair

    # Merge the two streams
    merged_stream = _merge_two_streams(streams[i], streams[j], file1, file2)

    # Create a new list of streams with the merged one
    new_streams = [s for k, s in enumerate(streams) if k != i and k != j]
    new_streams.append(merged_stream)

    logging.info(
        f"Merged two data streams with similar timing.\n"
        f"  Stream {i+1}: {streams[i].get('stream_modalities', 'unknown')}\n"
        f"  Stream {j+1}: {streams[j].get('stream_modalities', 'unknown')}"
    )

    # Recursively continue merging
    return _merge_data_streams(new_streams, file1, file2)


def _merge_dicts(
    dict1: Dict[str, Any], dict2: Dict[str, Any], file1: str, file2: str
) -> Dict[str, Any]:
    """
    Recursively merge two dictionaries.

    Handles special cases for certain fields.

    Parameters
    ----------
    dict1 : dict
        First dictionary.
    dict2 : dict
        Second dictionary.
    file1 : str
        Name of the first file.
    file2 : str
        Name of the second file.

    Returns
    -------
    dict
        The merged dictionary.
    """
    merged = {}
    all_keys = set(dict1.keys()) | set(dict2.keys())

    for key in all_keys:
        if key in dict1 and key in dict2:
            # Special case for reward_consumed_unit: pass parent dicts
            if key == "reward_consumed_unit":
                merged[key] = _merge_reward_unit(dict1, dict2, file1, file2)
            # Special case for data_streams: merge streams with similar timing
            elif key == "data_streams":
                # Combine all streams from both files and recursively merge
                all_streams = dict1[key] + dict2[key]
                merged[key] = _merge_data_streams(all_streams, file1, file2)
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
    If both totals are real, falls back to normal merge.

    Parameters
    ----------
    dict1 : dict
        First dictionary.
    dict2 : dict
        Second dictionary.
    file1 : str
        Name of the first file.
    file2 : str
        Name of the second file.

    Returns
    -------
    str
        The merged reward_consumed_unit value.
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

    Parameters
    ----------
    session_file1 : Union[str, Path]
        Path to first session JSON file
    session_file2 : Union[str, Path]
        Path to second session JSON file
    output_file : Union[str, Path]
        Path where merged session JSON will be saved

    Returns
    -------
    Dict[str, Any]
        Dictionary containing merged session metadata

    Raises
    ------
    ValueError
        If files cannot be read or merged
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
