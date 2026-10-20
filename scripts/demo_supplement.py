"""Supplement one session's acquisition from the metadata service and report what changed.

Pass a data asset name, for example ``behavior_869134_2026-09-28_09-35-27``. The script reads that
record's ``acquisition`` and ``data_description.modalities`` from the v2 DocDB REST API, runs
``aind_metadata_mapper.supplement.supplement_acquisition`` on it, and prints the supplemented fields
before and after. A record with no in vivo modality is printed unchanged. With ``--output-dir`` it
validates the result and writes it as ``acquisition.json`` there through
``Acquisition.write_standard_file``. Nothing is written back to DocDB. This is the shape a back-fill
migration would copy.
"""

import argparse
import copy
import json
from pathlib import Path

import requests
from aind_data_schema.core.acquisition import Acquisition

from aind_metadata_mapper.constants import METADATA_SERVICE_URL
from aind_metadata_mapper.models import SupplementSettings
from aind_metadata_mapper.supplement import supplement_acquisition

DOCDB_FIND_URL = "https://api.allenneuraldynamics.org/v2/metadata_index/data_assets/find"
SUPPLEMENTED_FIELDS = (
    "subject_details.animal_weight_prior",
    "subject_details.animal_weight_post",
    "ethics_review_id",
)


def fetch_record(name: str) -> dict:
    """Return the name, acquisition and data description modalities of the v2 DocDB record with this name."""
    params = {
        "filter": json.dumps({"name": name}),
        "projection": json.dumps({"name": 1, "acquisition": 1, "data_description.modalities": 1}),
        "limit": "1",
    }
    response = requests.get(DOCDB_FIND_URL, params=params, timeout=60)
    response.raise_for_status()
    records = response.json()
    if not records:
        raise SystemExit(f"No DocDB record named {name}.")
    return records[0]


def _get_field(acquisition: dict, dotted_path: str):
    """Return a nested acquisition value by dotted path, or None if any level is missing."""
    value = acquisition
    for key in dotted_path.split("."):
        value = (value or {}).get(key)
    return value


def main() -> None:
    """Fetch a session by name, supplement its acquisition, print what changed, and optionally write the result."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="Data asset name, for example behavior_869134_2026-09-28_09-35-27")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory to write the supplemented acquisition.json to. Without it the script only prints.",
    )
    parser.add_argument(
        "--metadata-service-url",
        default=METADATA_SERVICE_URL,
        help="Where the steps fetch weights and the IACUC protocol from. Requires the Allen network.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help=(
            "Let the steps overwrite values the acquisition already has with the fetched ones. "
            "Without it (the default, and what the GatherMetadataJob does) an existing value is kept and a "
            "mismatch is only logged as a warning. This changes the printed result and the --output-dir file "
            "only. Nothing is written back to DocDB either way."
        ),
    )
    args = parser.parse_args()

    record = fetch_record(args.name)
    acquisition = record.get("acquisition")
    if acquisition is None:
        raise SystemExit(f"{args.name} has no acquisition metadata.")

    data_description = record.get("data_description") or {}
    before = copy.deepcopy(acquisition)
    supplemented = supplement_acquisition(
        acquisition=acquisition,
        data_description=data_description,
        subject_id=acquisition.get("subject_id"),
        settings=SupplementSettings(replace_existing=args.replace_existing),
        metadata_service_url=args.metadata_service_url,
        raise_if_invalid=True,
    )
    result = {
        "name": args.name,
        "modalities": [modality.get("abbreviation") for modality in data_description.get("modalities") or []],
        "before": {field: _get_field(before, field) for field in SUPPLEMENTED_FIELDS},
        "after": {field: _get_field(supplemented, field) for field in SUPPLEMENTED_FIELDS},
    }
    print(json.dumps(result, indent=2))
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        Acquisition.model_validate(supplemented).write_standard_file(output_directory=args.output_dir)
        print(f"Wrote {args.output_dir / Acquisition.default_filename()}")


if __name__ == "__main__":
    main()
