"""Populate acquisition metadata with data that exists at upload time but was not supplied by the rig.

``GatherMetadataJob.run_job`` calls :func:`populate_acquisition` once, after the core metadata files
are assembled and before the final validation. The same function is meant to be imported by
migration scripts to back-fill existing records (see ``scripts/demo_populate_acquisition.py``).

The step runs only when ``data_description.modalities`` includes at least one in vivo modality
(``constants.IN_VIVO_MODALITIES``). Weights and IACUC protocols describe a living animal, so a
post-mortem acquisition such as SPIM gets neither.

Contract. Every step must satisfy all of these:

1. Fills only fields that are currently ``None`` or absent. Never overwrites unless
   ``replace_existing=True``.
2. If a value is already present and differs from the fetched value, logs a warning and keeps the
   existing value (unless ``replace_existing=True``).
3. Touches only schema-optional fields. The rig's ``acquisition.json`` already validated, so nothing
   a step adds can be required.
4. Fetches only through ``aind-metadata-service`` endpoints. No direct Dataverse or LabTracks table
   access.
5. A failed or empty lookup is a warning, never an exception.
6. Idempotent. Running twice produces the same result, so the same function can back-fill records
   later.
7. Not platform or task specific.

Steps run in the order listed in ``POPULATE_STEPS``:

- :func:`add_subject_weights` fills ``subject_details.animal_weight_prior`` and
  ``subject_details.animal_weight_post`` from Waterlog
  (metadata-service ``/api/v2/dataverse/mouse_weight_records/{subject_id}``).
- :func:`add_ethics_review_id` fills ``ethics_review_id`` from the LabTracks protocol number
  (metadata-service ``/api/v2/labtracks/subject?subject_id=``).
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Optional
from urllib.parse import urlencode, urljoin

from aind_data_schema.core.acquisition import AcquisitionSubjectDetails
from pydantic import ValidationError

from aind_metadata_mapper.constants import (
    IN_VIVO_MODALITIES,
    LABTRACKS_SUBJECT_ENDPOINT,
    MOUSE_WEIGHT_RECORDS_ENDPOINT,
)
from aind_metadata_mapper.models import PopulateAcquisitionSettings
from aind_metadata_mapper.utils import (
    ensure_timezone,
    get_iacuc_protocol,
    metadata_service_helper,
    normalize_utc_timezone,
)


def _parse_weight_record(weight_record: Any) -> Optional[tuple[datetime, Any]]:
    """Parse a usable Waterlog weight record."""
    if not isinstance(weight_record, dict):
        return None

    weight_datetime_value = weight_record.get("weight_datetime")
    weight = weight_record.get("weight")
    if not isinstance(weight_datetime_value, str) or weight is None:
        return None

    try:
        weight_datetime = ensure_timezone(datetime.fromisoformat(normalize_utc_timezone(weight_datetime_value)))
    except ValueError:
        return None
    return weight_datetime, weight


def _find_nearest_weights(
    weight_records: list[Any],
    acquisition_start: datetime,
) -> tuple[Optional[tuple[datetime, Any]], Optional[tuple[datetime, Any]]]:
    """Return the nearest records at or before and after acquisition start."""
    pre_weight_record = None
    post_weight_record = None
    for weight_record in weight_records:
        parsed_record = _parse_weight_record(weight_record)
        if parsed_record is None:
            continue

        weight_datetime, _ = parsed_record
        if weight_datetime <= acquisition_start:
            if pre_weight_record is None or weight_datetime > pre_weight_record[0]:
                pre_weight_record = parsed_record
        elif weight_datetime > acquisition_start:
            if post_weight_record is None or weight_datetime < post_weight_record[0]:
                post_weight_record = parsed_record
    return pre_weight_record, post_weight_record


def _apply_weight_records(
    subject_details: dict,
    weight_records: tuple[tuple[str, Optional[tuple[datetime, Any]]], ...],
    replace_existing: bool,
) -> list[str]:
    """Set each fetched weight on ``subject_details`` per the contract and return the fields filled."""
    filled_fields = []
    for weight_field, weight_record in weight_records:
        if weight_record is None:
            continue
        fetched_weight = weight_record[1]
        existing_weight = subject_details.get(weight_field)
        if existing_weight is None or replace_existing:
            subject_details[weight_field] = fetched_weight
            filled_fields.append(weight_field)
        elif Decimal(str(existing_weight)) != Decimal(str(fetched_weight)):
            logging.warning(
                f"Waterlog {weight_field} ({fetched_weight}) differs from existing {weight_field} "
                f"({existing_weight}); retaining the existing value."
            )
    return filled_fields


def add_subject_weights(
    acquisition: dict,
    subject_id: str,
    metadata_service_url: str,
    raise_if_invalid: bool = False,
    replace_existing: bool = False,
) -> dict:
    """Fill ``subject_details.animal_weight_prior`` and ``animal_weight_post`` from Waterlog.

    The prior weight is the latest same-day record at or before ``acquisition_start_time`` and the
    post weight is the earliest same-day record after it. Returns ``acquisition``, modified in place.
    """
    acquisition_start_value = acquisition.get("acquisition_start_time")
    if not acquisition_start_value:
        return acquisition
    acquisition_start_naive = datetime.fromisoformat(normalize_utc_timezone(acquisition_start_value))
    acquisition_start = ensure_timezone(acquisition_start_naive)

    acquisition_datetime = acquisition_start_naive.replace(tzinfo=None).isoformat(timespec="seconds")
    weight_records_url = urljoin(metadata_service_url, f"{MOUSE_WEIGHT_RECORDS_ENDPOINT}/{subject_id}")
    weight_records_url = f"{weight_records_url}?{urlencode({'acquisition_datetime': acquisition_datetime})}"
    weight_records = metadata_service_helper(weight_records_url)

    pre_weight_record, post_weight_record = _find_nearest_weights(
        weight_records if isinstance(weight_records, list) else [], acquisition_start
    )
    if pre_weight_record is None and post_weight_record is None:
        logging.warning(f"No Waterlog weight records found for subject {subject_id} around {acquisition_datetime}.")
        return acquisition
    subject_details = (acquisition.get("subject_details") or {}).copy()
    filled_fields = _apply_weight_records(
        subject_details,
        (("animal_weight_prior", pre_weight_record), ("animal_weight_post", post_weight_record)),
        replace_existing,
    )
    try:
        updated_details = AcquisitionSubjectDetails(**subject_details)
    except ValidationError as error:
        if raise_if_invalid:
            raise
        logging.warning(f"Unable to populate acquisition subject weights: {error}")
        return acquisition
    acquisition["subject_details"] = updated_details.model_dump(mode="json")
    if filled_fields:
        logging.info(f"Populated acquisition.subject_details {filled_fields} from Waterlog.")
    return acquisition


def add_ethics_review_id(
    acquisition: dict,
    subject_id: str,
    metadata_service_url: str,
    raise_if_invalid: bool = False,
    replace_existing: bool = False,
) -> dict:
    """Fill ``ethics_review_id`` with the subject's current IACUC protocol number from LabTracks.

    A mismatch between an existing value and LabTracks is a warning, not an error. Returns
    ``acquisition``, modified in place.
    """
    protocol_number = get_iacuc_protocol(subject_id, base_url=urljoin(metadata_service_url, LABTRACKS_SUBJECT_ENDPOINT))
    if protocol_number is None:
        logging.warning(
            f"No IACUC protocol found in LabTracks for subject {subject_id}; leaving ethics_review_id as is."
        )
        return acquisition
    fetched = [protocol_number]
    existing = acquisition.get("ethics_review_id")
    if not existing or replace_existing:
        acquisition["ethics_review_id"] = fetched
        logging.info(f"Populated acquisition.ethics_review_id with {fetched} from LabTracks.")
    elif set(existing) != set(fetched):
        logging.warning(
            f"LabTracks ethics_review_id {fetched} differs from existing {existing}; retaining the existing value."
        )
    return acquisition


POPULATE_STEPS: list[Callable[..., dict]] = [add_subject_weights, add_ethics_review_id]


def _modality_abbreviations(data_description: Optional[dict]) -> set[str]:
    """Return the modality abbreviations listed in ``data_description``, given as dicts, models or strings."""
    modalities = (data_description or {}).get("modalities") or []
    return {m.get("abbreviation") if isinstance(m, dict) else getattr(m, "abbreviation", m) for m in modalities}


def populate_acquisition(
    acquisition: Optional[dict],
    data_description: Optional[dict],
    subject_id: str,
    settings: PopulateAcquisitionSettings,
    metadata_service_url: str,
    raise_if_invalid: bool = False,
) -> Optional[dict]:
    """Apply every enabled step to ``acquisition`` and return it.

    Returns ``acquisition`` unchanged when it is None, when ``settings.enabled`` is False, when
    ``subject_id`` is ``"calibration"`` (nothing to look up), or when ``data_description.modalities``
    has no in vivo modality (see ``IN_VIVO_MODALITIES``).
    """
    if acquisition is None or not settings.enabled:
        return acquisition
    if subject_id == "calibration":
        logging.info("Skipping populate_acquisition: subject_id is 'calibration'.")
        return acquisition
    modalities = _modality_abbreviations(data_description)
    if not modalities & IN_VIVO_MODALITIES:
        logging.info(f"Skipping populate_acquisition: no in vivo modality in {sorted(map(str, modalities))}.")
        return acquisition
    for step in POPULATE_STEPS:
        acquisition = step(
            acquisition=acquisition,
            subject_id=subject_id,
            metadata_service_url=metadata_service_url,
            raise_if_invalid=raise_if_invalid,
            replace_existing=settings.replace_existing,
        )
    return acquisition
