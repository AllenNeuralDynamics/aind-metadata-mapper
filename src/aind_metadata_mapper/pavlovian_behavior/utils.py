"""Utility functions for Pavlovian behavior metadata extraction.

This module provides functions for extracting and processing data
from Pavlovian behavior files. Functions are organized
by their specific tasks:
- File discovery and validation
- Timestamp parsing and manipulation
- Trial data extraction and processing
- Session timing calculations
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Optional
from zoneinfo import ZoneInfo
import pandas as pd
from aind_data_schema.core.session import StimulusEpoch


def find_behavior_files(
    data_dir: Path,
) -> Tuple[List[Path], List[Path]]:
    """Find behavior and trial files in the data directory.

    Searches for behavior files (TS_CS1_*.csv) and trial files
    (TrialN_TrialType_ITI_*.csv) in the provided directory or its 'behavior'
    subdirectory.

    Args:
        data_dir: Base directory to search for files

    Returns:
        Tuple containing:
            - List of paths to behavior files
            - List of paths to trial files

    Raises:
        FileNotFoundError: If required files are not found
        ValueError: If file names do not match expected format
    """
    behavior_dir = data_dir / "behavior"
    if not behavior_dir.exists():
        behavior_dir = data_dir

    behavior_files = list(behavior_dir.glob("TS_CS1_*.csv"))
    trial_files = list(behavior_dir.glob("TrialN_TrialType_ITI_*.csv"))

    if not behavior_files:
        raise FileNotFoundError(
            f"No behavior files (TS_CS1_*.csv) found in {behavior_dir}"
        )
    if not trial_files:
        raise FileNotFoundError(
            "No trial files (TrialN_TrialType_ITI_*.csv) "
            f"found in {behavior_dir}"
        )

    # Validate all found files
    for bf in behavior_files:
        validate_behavior_file_format(bf)
    for tf in trial_files:
        validate_trial_file_format(tf)

    return behavior_files, trial_files


def parse_session_start_time(
    behavior_file: Path, local_timezone: Optional[str] = None
) -> datetime:
    """Parse session start time from behavior file name.

    Args:
        behavior_file: Path to behavior file
        local_timezone: Optional timezone string. If not provided, UTC is used.

    Returns:
        datetime: Session start time in UTC

    Raises:
        ValueError: If datetime cannot be parsed from filename
    """
    try:
        # Extract timestamp part from filename
        # (e.g. TS_CS1_2024-12-31T15_49_53.csv)
        parts = behavior_file.stem.split(
            "_", 2
        )  # Split on first two underscores only
        if len(parts) < 3:
            raise ValueError(f"Invalid filename format: {behavior_file.name}")

        # Get the datetime part and parse it
        date_time_str = parts[2]  # e.g. "2024-12-31T15_49_53"
        # Replace underscores with colons for proper datetime parsing
        date_time_str = date_time_str.replace("_", ":")
        parsed_time = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S")

        # Convert to UTC
        local_time = parsed_time.replace(
            tzinfo=ZoneInfo(local_timezone if local_timezone else "UTC")
        )
        return local_time.astimezone(ZoneInfo("UTC"))

    except (ValueError, IndexError) as e:
        raise ValueError(
            f"Could not parse datetime from filename: {behavior_file.name}"
        ) from e


def extract_trial_data(
    trial_file: Path,
) -> pd.DataFrame:
    """Read and validate trial data from CSV file.

    Args:
        trial_file: Path to trial data CSV file

    Returns:
        DataFrame containing trial data

    Raises:
        ValueError: If required columns are missing
    """
    trial_data = pd.read_csv(trial_file)
    required_columns = ["TrialNumber", "TotalRewards", "ITI_s"]

    missing_cols = [col for col in required_columns if col not in trial_data]
    if missing_cols:
        raise ValueError(
            f"Trial data missing required columns: {', '.join(missing_cols)}"
        )

    return trial_data


def calculate_session_timing(
    start_time: datetime,
    trial_data: pd.DataFrame,
) -> Tuple[datetime, float]:
    """Calculate session end time and duration from trial data.

    Args:
        start_time: Session start time (in UTC)
        trial_data: DataFrame containing trial information

    Returns:
        Tuple containing:
            - Session end time (in UTC)
            - Total session duration in seconds
    """
    total_duration = float(trial_data["ITI_s"].sum())
    end_time = start_time + timedelta(seconds=total_duration)

    return end_time, total_duration


def create_stimulus_epoch(
    start_time: datetime,
    end_time: datetime,
    trial_data: pd.DataFrame,
    reward_units_per_trial: float = 2.0,
    stimulus_name: str = "Pavlovian",
) -> StimulusEpoch:
    """Create a StimulusEpoch object from trial information.

    Args:
        start_time: Epoch start time
        end_time: Epoch end time
        trial_data: DataFrame containing trial information
        reward_units_per_trial: Units of reward given per successful trial
        stimulus_name: Name of the stimulus protocol

    Returns:
        StimulusEpoch object with trial and reward information
    """
    total_trials = int(trial_data["TrialNumber"].iloc[-1])
    total_rewards = int(trial_data["TotalRewards"].iloc[-1])

    return StimulusEpoch(
        stimulus_name=stimulus_name,
        stimulus_start_time=start_time,
        stimulus_end_time=end_time,
        stimulus_modalities=["Auditory"],
        trials_finished=total_trials,
        trials_total=total_trials,
        trials_rewarded=total_rewards,
        reward_consumed_during_epoch=total_rewards * reward_units_per_trial,
    )


def extract_session_data(
    data_dir: Path,
    reward_units_per_trial: float = 2.0,
    local_timezone: Optional[str] = None,
) -> Tuple[datetime, List[StimulusEpoch]]:
    """Extract all session data from behavior files.

    This is the main entry point for data extraction, coordinating the use of
    other utility functions to gather all required session information.

    Args:
        data_dir: Directory containing behavior files
        reward_units_per_trial: Units of reward given per successful trial
        local_timezone: Optional timezone string. If not provided, will use
            system timezone.

    Returns:
        Tuple containing:
            - Session start time (in UTC)
            - List of StimulusEpoch objects

    Raises:
        FileNotFoundError: If required files are not found
        ValueError: If data parsing or extraction fails
    """
    # Find required files
    behavior_files, trial_files = find_behavior_files(data_dir)

    # Parse session start time
    start_time = parse_session_start_time(
        behavior_files[0], local_timezone=local_timezone
    )

    # Extract trial data
    trial_data = extract_trial_data(trial_files[0])

    # Calculate session timing
    end_time, _ = calculate_session_timing(start_time, trial_data)

    # Create stimulus epoch
    stimulus_epoch = create_stimulus_epoch(
        start_time,
        end_time,
        trial_data,
        reward_units_per_trial,
    )

    return start_time, [stimulus_epoch]


def validate_behavior_file_format(behavior_file: Path) -> None:
    """Validate that behavior file name matches expected format.

    Expected format:
    - TS_CS1_YYYY-MM-DDThh_mm_ss.csv
    Example: TS_CS1_2024-01-01T15_49_53.csv

    Args:
        behavior_file: Path to behavior file

    Raises:
        ValueError: If file name does not match expected format, with detailed
            explanation of what was wrong
    """
    name = behavior_file.name
    parts = name.split("_", 2)

    # Check basic structure
    if len(parts) != 3:
        raise ValueError(
            f"Invalid behavior file name: {name}\n"
            "Expected format: TS_CS1_YYYY-MM-DDThh_mm_ss.csv\n"
            "Example: TS_CS1_2024-01-01T15_49_53.csv\n"
            "Error: File name should have exactly three parts\n"
            "separated by underscores"
        )

    if parts[0] != "TS" or parts[1] != "CS1":
        raise ValueError(
            f"Invalid behavior file name: {name}\n"
            "Expected format: TS_CS1_YYYY-MM-DDThh_mm_ss.csv\n"
            "Example: TS_CS1_2024-01-01T15_49_53.csv\n"
            "Error: File name must start with 'TS_CS1_'"
        )

    # Check datetime part
    datetime_part = parts[2]
    if not datetime_part.endswith(".csv"):
        raise ValueError(
            f"Invalid behavior file name: {name}\n"
            "Expected format: TS_CS1_YYYY-MM-DDThh_mm_ss.csv\n"
            "Example: TS_CS1_2024-01-01T15_49_53.csv\n"
            "Error: File must have .csv extension"
        )

    datetime_str = datetime_part[:-4]  # Remove .csv

    # Check date format
    try:
        # Try to parse the datetime to validate format
        date_time_str = datetime_str.replace("_", ":")
        datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise ValueError(
            f"Invalid behavior file name: {name}\n"
            "Expected format: TS_CS1_YYYY-MM-DDThh_mm_ss.csv\n"
            "Example: TS_CS1_2024-01-01T15_49_53.csv\n"
            "Error: Date/time part must be in format YYYY-MM-DDThh_mm_ss"
        )


def validate_trial_file_format(trial_file: Path) -> None:
    """Validate that trial file name matches expected format.

    Expected format:
    - TrialN_TrialType_ITI_*.csv
    Example: TrialN_TrialType_ITI_001.csv

    Args:
        trial_file: Path to trial file

    Raises:
        ValueError: If file name does not match expected format, with detailed
            explanation of what was wrong
    """
    name = trial_file.name
    parts = name.split("_")

    if len(parts) < 4:
        raise ValueError(
            f"Invalid trial file name: {name}\n"
            "Expected format: TrialN_TrialType_ITI_*.csv\n"
            "Example: TrialN_TrialType_ITI_001.csv\n"
            "Error: File name should have at least\n"
            "four parts separated by underscores"
        )

    if parts[0] != "TrialN" or parts[1] != "TrialType" or parts[2] != "ITI":
        raise ValueError(
            f"Invalid trial file name: {name}\n"
            "Expected format: TrialN_TrialType_ITI_*.csv\n"
            "Example: TrialN_TrialType_ITI_001.csv\n"
            "Error: File name must start with 'TrialN_TrialType_ITI_'"
        )

    if not name.endswith(".csv"):
        raise ValueError(
            f"Invalid trial file name: {name}\n"
            "Expected format: TrialN_TrialType_ITI_*.csv\n"
            "Example: TrialN_TrialType_ITI_001.csv\n"
            "Error: File must have .csv extension"
        )
