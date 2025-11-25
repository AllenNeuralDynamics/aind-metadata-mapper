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
from aind_data_schema.core.acquisition import Acquisition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = "http://aind-metadata-service/api/v2/instrument"


def write_acquisition(
    model: Acquisition, output_directory: Optional[str] = None, filename: str = "acquisition.json"
) -> Path:
    """Write an Acquisition model to a JSON file.

    Parameters
    ----------
    model : Acquisition
        The acquisition model to write.
    output_directory : Optional[str], optional
        Output directory path, by default None (current directory).
    filename : str, optional
        Output filename, by default "acquisition.json".

    Returns
    -------
    Path
        Path to the written file.
    """
    if output_directory:
        output_path = Path(output_directory) / filename
    else:
        output_path = Path(filename)

    with open(output_path, "w") as f:
        f.write(model.model_dump_json(indent=2))

    logger.info(f"Wrote acquisition metadata to {output_path}")
    return output_path


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
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        # Use system's local timezone
        local_tz = datetime.now().astimezone().tzinfo
        dt = dt.replace(tzinfo=local_tz)
    return dt


def get_procedures(
    subject_id: str, get_func=None, service_url: str = "http://aind-metadata-service-dev/api/v2/procedures"
) -> Optional[dict]:
    """Fetch procedures data for a subject from the metadata service.

    Queries {service_url}/{subject_id} to get all procedures performed on a subject.
    Default service URL: http://aind-metadata-service-dev/api/v2/procedures

    Parameters
    ----------
    subject_id : str
        The subject ID to query.
    get_func : callable, optional
        Function to use for HTTP GET requests. If None, uses requests.get.
        Useful for testing without making real network calls.
    service_url : str, optional
        Base URL for the procedures service endpoint. Defaults to
        "http://aind-metadata-service-dev/api/v2/procedures".

    Returns
    -------
    Optional[dict]
        Procedures data dictionary, or None if the request fails.
    """
    if get_func is None:  # pragma: no cover
        get_func = requests.get

    try:
        # Ensure service_url ends with '/' for urljoin to work correctly
        base_url = service_url.rstrip("/") + "/"
        url = urljoin(base_url, subject_id.lstrip("/"))
        response = get_func(url, timeout=60)

        # Handle 400 status codes (normal for this API) and successful responses (2xx)
        status_code = response.status_code
        if isinstance(status_code, int):
            if status_code == 400 or (200 <= status_code < 300):
                return response.json()
        else:
            # For test mocks that don't set status_code as int, try to return JSON
            # This handles cases where the mock response is successful
            try:
                return response.json()
            except Exception:
                pass

        # For other status codes, log warning and return None
        logger.warning(f"Could not fetch procedures for subject {subject_id} " f"(status {status_code})")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Could not fetch procedures for subject {subject_id}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error fetching procedures for subject {subject_id}: {e}")
        return None


def get_intended_measurements(subject_id: str, get_func=None) -> Optional[dict]:
    """Fetch intended measurements for a subject from the metadata service.

    Queries http://aind-metadata-service/intended_measurements/{subject_id}
    to get the measurement assignments.

    Parameters
    ----------
    subject_id : str
        The subject ID to query.
    get_func : callable, optional
        Function to use for HTTP GET requests. If None, uses requests.get.
        Useful for testing without making real network calls.

    Returns
    -------
    Optional[dict]
        Intended measurements data dictionary, or None if the request fails.
    """
    if get_func is None:
        get_func = requests.get

    try:
        url = f"http://aind-metadata-service/intended_measurements/{subject_id}"
        response = get_func(url, timeout=5)

        if response.status_code not in [200, 300]:
            logger.warning(
                f"Could not fetch intended measurements for subject {subject_id} " f"(status {response.status_code})"
            )
            return None

        return response.json()

    except Exception as e:
        logger.warning(f"Error fetching intended measurements for subject {subject_id}: {e}")
        return None


def load_protocols():
    """Load protocol URLs from protocols.yaml file.

    Returns
    -------
    dict
        Dictionary mapping modality names to lists of protocol URLs.
    """
    try:
        project_root = Path(__file__).parent.parent.parent
        protocols_file = project_root / "protocols.yaml"

        if not protocols_file.exists():
            logger.warning(f"Protocols file not found at {protocols_file}")
            return {}

        with open(protocols_file, "r") as f:
            protocols = yaml.safe_load(f)

        return protocols or {}
    except Exception as e:
        logger.warning(f"Error loading protocols: {e}")
        return {}


def get_protocols_for_modality(modality):
    """Get protocol URLs for a specific modality.

    Parameters
    ----------
    modality : str
        The modality name (e.g., 'fip', 'smartspim').

    Returns
    -------
    list
        List of protocol URLs for the modality.
    """
    protocols = load_protocols()
    return protocols.get(modality, [])


def get_instrument(instrument_id: str, modification_date: Optional[str] = None) -> Optional[dict]:  # pragma: no cover
    """Get instrument.

    Gets the latest record by default, or a specific record if modification_date is provided.

    Parameters
    ----------
    instrument_id : str
        Instrument identifier.
    modification_date : Optional[str]
        Optional modification date (YYYY-MM-DD format). If None, returns latest record.

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
        # No matching date found - log available dates
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


def save_instrument(instrument_model: instrument.Instrument) -> None:  # pragma: no cover
    """Save instrument and validate round-trip.

    Saves the instrument, then retrieves it back and verifies that what we get back
    matches what we just saved. If so, we know the save worked.

    Parameters
    ----------
    instrument_model : instrument.Instrument
        Instrument to POST.

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
    response = requests.post(API_BASE_URL, json=source_dict)
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
