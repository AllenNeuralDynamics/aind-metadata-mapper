"""Enrich one session's acquisition from the metadata service and report what changed.

Pass a data asset name, for example ``behavior_869134_2026-09-28_09-35-27``. The script reads that
record's ``acquisition`` and ``subject`` from the v2 DocDB REST API, runs
``aind_metadata_mapper.enrichment.enrich_acquisition`` on it (which validates the acquisition before
and after enriching), and prints the enriched fields before and after. With ``--output-dir`` it also
writes the enriched ``acquisition.json`` there through ``Acquisition.write_standard_file``. Nothing is
written back to DocDB. This is the shape a back-fill migration would copy.
"""

import argparse
import copy
import json
from pathlib import Path

import requests

from aind_metadata_mapper.enrichment import enrich_acquisition, write_acquisition
from aind_metadata_mapper.models import EnrichmentSettings

DOCDB_FIND_URL = "https://api.allenneuraldynamics.org/v2/metadata_index/data_assets/find"
ENRICHED_FIELDS = (
    "subject_details.animal_weight_prior",
    "subject_details.animal_weight_post",
    "ethics_review_id",
)


def fetch_record(name: str) -> dict:
    """Return the subject and acquisition of the v2 DocDB record with this name."""
    params = {
        "filter": json.dumps({"name": name}),
        "projection": json.dumps({"name": 1, "subject": 1, "acquisition": 1}),
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
    """Fetch a session by name, enrich its acquisition, print the enriched fields, and optionally write the result."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="Data asset name, for example behavior_869134_2026-09-28_09-35-27")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory to write the enriched acquisition.json to. Without it the script only prints.",
    )
    parser.add_argument(
        "--metadata-service-url",
        default="http://aind-metadata-service",
        help="Where the enrichers fetch weights and the IACUC protocol from. Requires the Allen network.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help=(
            "Let the enrichers overwrite values the acquisition already has with the fetched ones. "
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
    subject = record.get("subject")

    before = copy.deepcopy(acquisition)
    enriched = enrich_acquisition(
        acquisition=acquisition,
        subject=subject,
        subject_id=acquisition.get("subject_id") or (subject or {}).get("subject_id"),
        settings=EnrichmentSettings(replace_existing=args.replace_existing),
        metadata_service_url=args.metadata_service_url,
        raise_if_invalid=True,
    )
    result = {
        "name": args.name,
        "before": {field: _get_field(before, field) for field in ENRICHED_FIELDS},
        "after": {field: _get_field(enriched, field) for field in ENRICHED_FIELDS},
    }
    print(json.dumps(result, indent=2))
    if args.output_dir:
        print(f"Wrote {write_acquisition(enriched, args.output_dir)}")


if __name__ == "__main__":
    main()
