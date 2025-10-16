"""Utility functions for AIND metadata mappers."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from aind_data_schema.core.acquisition import Acquisition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def get_procedures(subject_id: str) -> Optional[dict]:
    """Fetch procedures data for a subject from the metadata service.

    Queries http://aind-metadata-service-dev/api/v2/procedures/{subject_id}
    to get all procedures performed on a subject.

    Parameters
    ----------
    subject_id : str
        The subject ID to query.

    Returns
    -------
    Optional[dict]
        Procedures data dictionary, or None if the request fails.
    """
    try:
        url = f"http://aind-metadata-service-dev/api/v2/procedures/{subject_id}"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(f"Could not fetch procedures for subject {subject_id}: {e}")
        return None


def get_intended_measurements(subject_id: str) -> Optional[dict]:
    """Fetch intended measurements for a subject from the metadata service.

    Queries http://aind-metadata-service/intended_measurements/{subject_id}
    to get the measurement assignments.

    Parameters
    ----------
    subject_id : str
        The subject ID to query.

    Returns
    -------
    Optional[dict]
        Intended measurements data dictionary, or None if the request fails.
    """
    try:
        url = f"http://aind-metadata-service/intended_measurements/{subject_id}"
        response = requests.get(url, timeout=5)

        if response.status_code not in [200, 300]:
            logger.warning(
                f"Could not fetch intended measurements for subject {subject_id} " f"(status {response.status_code})"
            )
            return None

        return response.json()

    except Exception as e:
        logger.warning(f"Error fetching intended measurements for subject {subject_id}: {e}")
        return None
