"""Utility functions for Pavlovian behavior metadata extraction.

This module provides functions for extracting and processing data from Pavlovian
behavior files. Functions are organized by their specific tasks:
- File discovery and validation
- Timestamp parsing and manipulation
- Trial data extraction and processing
- Session timing calculations
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from zoneinfo import ZoneInfo
from tzlocal import get_localzone
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
            f"No trial files (TrialN_TrialType_ITI_*.csv) found in {behavior_dir}"
        )

    return behavior_files, trial_files


def parse_session_start_time(
    behavior_file: Path, local_timezone: Optional[str] = None
) -> datetime:
    """Extract and parse session start time from behavior filename.

    Args:
        behavior_file: Path to behavior file with timestamp in name
        local_timezone: Optional timezone string. If not provided, will use
            system timezone.

    Returns:
        Timezone-aware datetime object in UTC

    Raises:
        ValueError: If timestamp cannot be parsed from filename
    """
    raw_time_part = "_".join(behavior_file.stem.split("_")[2:])

    try:
        parsed_time = datetime.strptime(raw_time_part, "%Y-%m-%dT%H_%M_%S")
        if parsed_time.tzinfo is None:
            # Get timezone
            tz = (
                ZoneInfo(local_timezone) if local_timezone else get_localzone()
            )
            # Set to local time then convert to UTC
            local_time = parsed_time.replace(tzinfo=tz)
            return local_time.astimezone(ZoneInfo("UTC"))
    except ValueError:
        raise ValueError(
            f"Could not parse datetime from filename: {behavior_file.name}"
        )


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
