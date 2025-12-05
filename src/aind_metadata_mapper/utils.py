"""Utility functions for AIND metadata mappers."""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import aind_data_schema.core.instrument as instrument
import requests
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INSTRUMENT_BASE_URL = "http://aind-metadata-service/api/v2/instrument"
PROCEDURES_BASE_URL = "http://aind-metadata-service/api/v2/procedures"
SUBJECT_BASE_URL = "http://aind-metadata-service/api/v2/subject"


def ensure_timezone(dt):
    """Ensure datetime has timezone info using system local timezone.

    Parameters
    ----------
    dt : datetime, str, or None
        Datetime to process. Can be a datetime object, ISO format string, or None.

    Returns
    -------
    datetime
        Datetime with timezone info. If None is provided, returns current time
        in local timezone.
    """
    if dt is None:
        return datetime.now().astimezone()
    if isinstance(dt, str):
        dt.replace("Z", "+00:00")  # Handle UTC 'Z' suffix for Python < 3.11
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        # Use system's local timezone
        local_tz = datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
    return dt


def metadata_service_helper(url: str, timeout: int = 60) -> Optional[dict]:
    """Fetch metadata from a service URL.

    Parameters
    ----------
    url : str
        Full URL to fetch metadata from.
    timeout : int
        Request timeout in seconds. Default is 60.

    Returns
    -------
    Optional[dict]
        Metadata as a dictionary, or None if error occurs.
    """
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code == 400:
            return response.json()
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error retrieving metadata from {url}: {e}")
        return None


def get_subject(subject_id: str, base_url: str = SUBJECT_BASE_URL) -> Optional[dict]:
    """Fetch subject data from the metadata service.

    Parameters
    ----------
    subject_id : str
        The subject ID to query.
    base_url : str
        Base URL for the subject endpoint. Defaults to SUBJECT_BASE_URL.

    Returns
    -------
    Optional[dict]
        Subject data dictionary, or None if the request fails.
    """
    try:
        url_base = base_url.rstrip("/") + "/"
        url = urljoin(url_base, subject_id.lstrip("/"))
        result = metadata_service_helper(url)
        if result is None:
            logger.warning(f"Could not fetch subject {subject_id}")
        return result
    except Exception as e:
        logger.warning(f"Unexpected error fetching subject {subject_id}: {e}")
        return None


def get_procedures(subject_id: str, base_url: str = PROCEDURES_BASE_URL) -> Optional[dict]:
    """Fetch procedures data for a subject from the metadata service.

    Parameters
    ----------
    subject_id : str
        The subject ID to query.
    base_url : str
        Base URL for the procedures endpoint. Defaults to PROCEDURES_BASE_URL.

    Returns
    -------
    Optional[dict]
        Procedures data dictionary, or None if the request fails.
    """
    try:
        url_base = base_url.rstrip("/") + "/"
        url = urljoin(url_base, subject_id.lstrip("/"))
        result = metadata_service_helper(url)
        if result is None:
            logger.warning(f"Could not fetch procedures for subject {subject_id}")
        return result
    except Exception as e:
        logger.warning(f"Unexpected error fetching procedures for subject {subject_id}: {e}")
        return None


def get_intended_measurements(
    subject_id: str, base_url: str = "http://aind-metadata-service/intended_measurements"
) -> Optional[dict]:
    """Fetch intended measurements for a subject from the metadata service.

    Parameters
    ----------
    subject_id : str
        The subject ID to query.
    base_url : str
        Base URL for the intended measurements endpoint.

    Returns
    -------
    Optional[dict]
        Intended measurements data dictionary, or None if the request fails.
    """
    try:
        url = f"{base_url}/{subject_id}"
        result = metadata_service_helper(url, timeout=5)
        if result is None:
            logger.warning(f"Could not fetch intended measurements for subject {subject_id}")
        return result
    except Exception as e:
        logger.warning(f"Error fetching intended measurements for subject {subject_id}: {e}")
        return None


def get_protocols_for_modality(modality):
    """Get protocol URLs for a specific modality.

    Loads protocols from protocols.yaml file and returns the list for the given modality.

    Parameters
    ----------
    modality : str
        The modality name (e.g., 'fip', 'smartspim').

    Returns
    -------
    list
        List of protocol URLs for the modality.
    """
    try:
        project_root = Path(__file__).parent.parent.parent
        protocols_file = project_root / "protocols.yaml"

        if not protocols_file.exists():
            logger.warning(f"Protocols file not found at {protocols_file}")
            return []

        with open(protocols_file, "r") as f:
            protocols = yaml.safe_load(f)

        protocols = protocols or {}
        return protocols.get(modality, [])
    except Exception as e:
        logger.warning(f"Error loading protocols: {e}")
        return []


def get_instrument(
    instrument_id: str,
    modification_date: Optional[str] = None,
    suppress_warning: bool = False,
    base_url: str = INSTRUMENT_BASE_URL,
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
    base_url : str
        Base URL for the instrument endpoint. Defaults to INSTRUMENT_BASE_URL.

    Returns
    -------
    Optional[dict]
        Instrument data as dict, or None if not found.
    """
    try:
        url_base = base_url.rstrip("/") + "/"
        url = urljoin(url_base, instrument_id.lstrip("/"))
        url_with_params = f"{url}?partial_match=true"
        result = metadata_service_helper(url_with_params)
        if result is None:
            logger.warning(f"Could not fetch instrument {instrument_id}")
            return None

        records = result if isinstance(result, list) else [result]
        matching_records = [
            r for r in records if r.get("modification_date") and r.get("instrument_id") == instrument_id
        ]
        if not matching_records:
            return None

        if modification_date:
            for record in matching_records:
                if record.get("modification_date") == modification_date:
                    return record

            # No matching record found
            if not suppress_warning:
                available_dates = sorted(
                    set(r.get("modification_date") for r in matching_records if r.get("modification_date"))
                )
                logger.warning(
                    f"No record found for instrument_id '{instrument_id}' with "
                    f"modification_date '{modification_date}'. "
                    f"Available dates: {available_dates}"
                )
            return None
        else:
            return sorted(matching_records, key=lambda record: record["modification_date"])[-1]
    except Exception as e:
        logger.warning(f"Unexpected error fetching instrument {instrument_id}: {e}")
        return None


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
    logger.info(f"POSTing instrument to {INSTRUMENT_BASE_URL}")
    params = {"replace": "true"} if replace else {}
    response = requests.post(INSTRUMENT_BASE_URL, json=source_dict, params=params)
    # POST 400 is always an error (e.g., "Record already exists")
    if response.status_code == 400:
        error_msg = response.json().get("message", response.text)
        raise ValueError(f"Cannot POST instrument: {error_msg}")
    if response.status_code >= 500:
        response.raise_for_status()

    # GET back and validate round-trip
    logger.info(
        f"GETting instrument from {INSTRUMENT_BASE_URL}/{instrument_model.instrument_id} "
        "to verify that save was successful"
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
