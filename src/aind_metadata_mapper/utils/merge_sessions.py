"""Utility functions for merging multiple session metadata files."""

import json
import argparse
from pathlib import Path
from typing import Any, Dict, List, Union
import logging


def _merge_lists(list1: List[Any], list2: List[Any]) -> List[Any]:
    """Merge two lists, removing duplicates while preserving order."""
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
    print(f"\nConflict in '{field_name}':")
    print(f"  {file1}: {value1}")
    print(f"  {file2}: {value2}")
    print(f"\nDefault merged value: {default}")
    response = input("Accept default? [Y/n] or type new value: ").strip()

    if not response or response.lower() == "y":
        return default
    return response


def _merge_values(
    field: str, val1: Any, val2: Any, file1: str, file2: str
) -> Any:
    """Merge two values based on their types.

    Args:
        field: Name of the field being merged
        val1: Value from first file
        val2: Value from second file
        file1: Name of first file
        file2: Name of second file

    Returns:
        Merged value
    """
    # Handle case where one value is None
    if val1 is None:
        return val2
    if val2 is None:
        return val1

    # If values are identical, return either
    if val1 == val2:
        return val1

    # Handle different types based on their Python type
    if isinstance(val1, list) and isinstance(val2, list):
        return _merge_lists(val1, val2)
    elif isinstance(val1, dict) and isinstance(val2, dict):
        return merge_dicts(val1, val2, file1, file2)
    elif isinstance(val1, str) and isinstance(val2, str):
        return _prompt_for_field(field, val1, val2, file1, file2)
    else:
        # For other types (numbers, booleans), prompt user
        return _prompt_for_field(field, str(val1), str(val2), file1, file2)


def merge_dicts(
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
            merged[key] = _merge_values(
                key, dict1[key], dict2[key], file1, file2
            )
        else:
            # If key only exists in one dict, use that value
            merged[key] = dict1.get(key) or dict2.get(key)

    return merged


def merge_sessions(
    session_file1: Union[str, Path], session_file2: Union[str, Path]
) -> Dict[str, Any]:
    """Merge two session metadata files into a single session.

    Args:
        session_file1: Path to first session JSON file
        session_file2: Path to second session JSON file

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

    return merge_dicts(
        session1, session2, Path(session_file1).name, Path(session_file2).name
    )


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

    merged_session = merge_sessions(args.file1, args.file2)

    with open(args.output, "w") as f:
        json.dump(merged_session, f, indent=2)

    print(f"Merged session saved to: {args.output}")


if __name__ == "__main__":
    main()
