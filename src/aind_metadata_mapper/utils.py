"""General purpose utility functions"""

import json
import logging
import sys
from typing import Optional

import aind_data_schema.core.instrument as instrument
import requests

logger = logging.getLogger(__name__)

API_BASE_URL = "http://aind-metadata-service/api/v2/instrument"


def get_instrument(
    instrument_id: str, modification_date: Optional[str] = None, suppress_warning: bool = False
) -> Optional[dict]:  # pragma: no cover
    """Get instrument.

    Gets the latest record by default, or a specific record if modification_date is provided.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier.
    modification_date : Optional[str]
        Optional modification date (YYYY-MM-DD format). If None, returns latest record.
    suppress_warning : bool
        If True, suppress warning when modification_date not found.

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
    # Find records matching instrument_id
    matching_records = [r for r in records if r.get("modification_date") and r.get("instrument_id") == instrument_id]
    if not matching_records:
        return None

    if modification_date:
        # Return record matching specific date
        for record in matching_records:
            if record.get("modification_date") == modification_date:
                return record
        # No matching date found - log available dates (unless suppressed)
        if not suppress_warning:
            available_dates = sorted(
                set(r.get("modification_date") for r in matching_records if r.get("modification_date"))
            )
            logger.warning(
                f"No record found for instrument_id '{instrument_id}' with modification_date '{modification_date}'. "
                f"Available dates: {available_dates}"
            )
        return None
    else:
        # Return latest record
        return sorted(matching_records, key=lambda record: record["modification_date"])[-1]


def save_instrument(instrument_model: instrument.Instrument, replace: bool = False) -> None:  # pragma: no cover
    """Save instrument and validate round-trip.

    Saves the instrument, then retrieves it back and verifies that what we get back
    matches what we just saved. If so, we know the save worked.

    Parameters
    ----------
    instrument_model : instrument.Instrument
        Instrument to POST.
    replace : bool
        If True, overwrite existing record with same instrument_id and modification_date.

    Raises
    ------
    ValueError
        If POST fails (e.g., record already exists) or round-trip validation fails.
    requests.HTTPError
        If server error occurs (500+).
    """
    # Use model_dump_json() and parse to ensure dates are properly serialized
    source_dict = json.loads(instrument_model.model_dump_json())
    logger.info(f"POSTing instrument to {API_BASE_URL}")
    params = {"replace": "true"} if replace else {}
    response = requests.post(API_BASE_URL, json=source_dict, params=params)
    # POST 400 is always an error (e.g., "Record already exists")
    if response.status_code == 400:
        error_msg = response.json().get("message", response.text)
        raise ValueError(f"Cannot POST instrument: {error_msg}")
    if response.status_code >= 500:
        response.raise_for_status()

    # GET back and validate round-trip
    logger.info(
        f"GETting instrument from {API_BASE_URL}/{instrument_model.instrument_id} to verify that save was successful"
    )
    latest_record = get_instrument(instrument_model.instrument_id)
    if latest_record is None:
        raise ValueError(f"Instrument '{instrument_model.instrument_id}' not found in database")

    # Validate round-trip
    read_back_instrument = instrument.Instrument.model_validate(latest_record)
    read_back_dict = json.loads(read_back_instrument.model_dump_json())
    if source_dict == read_back_dict:
        logger.info("Instrument.json successfully stored in the db")
    else:
        raise ValueError("Round-trip test failed: Source and read-back instruments differ")


def check_existing_instrument(
    instrument_id: str,
    modification_date: str,
) -> bool:  # pragma: no cover
    """Check if an instrument with the same ID and modification_date already exists.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier.
    modification_date : str
        Modification date (YYYY-MM-DD format).

    Returns
    -------
    bool
        True if a record with the same instrument_id and modification_date exists.
    """
    existing = get_instrument(instrument_id, modification_date=modification_date, suppress_warning=True)
    return existing is not None


def check_instrument_id(
    instrument_id: str,
    skip_confirmation: bool = False,
    input_func=input,
) -> Optional[dict]:  # pragma: no cover
    """Check if instrument exists and get previous instrument data.

    Checks if records exist for the given instrument_id and returns the
    previous instrument if found. Confirms creation if instrument doesn't exist.

    Parameters
    ----------
    instrument_id : str
        Instrument ID to check.
    skip_confirmation : bool
        If True, skip confirmation prompt for new instrument IDs.
    input_func : callable
        Function to get user input. Defaults to builtin input().

    Returns
    -------
    Optional[dict]
        Previous instrument data as dict, or None if not found.

    Raises
    ------
    SystemExit
        If user cancels creation of new instrument ID.
    """
    # Check if instrument exists
    previous_instrument = get_instrument(instrument_id)
    if previous_instrument is None and not skip_confirmation:
        logger.info("This is a new instrument ID.")
        response = (
            input_func(f"Are you sure you want to create a new ID with name '{instrument_id}'? [Y/n]: ").strip().lower()
        )
        if response and response not in ("y", "yes"):
            logger.info("Cancelled. Please run the script again with the correct instrument ID.")
            sys.exit(0)

    return previous_instrument


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
