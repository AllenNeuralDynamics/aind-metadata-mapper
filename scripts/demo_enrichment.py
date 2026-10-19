"""Enrich one acquisition from the metadata service and print the before and after values.

This runs ``aind_metadata_mapper.enrichment.enrich_acquisition`` outside the GatherMetadataJob, which
is the pattern a back-fill migration script would follow: load an acquisition, fetch its subject,
call ``enrich_acquisition``, and compare.
"""

import argparse
import copy
import json
from urllib.parse import urljoin

from aind_metadata_mapper.enrichment import enrich_acquisition
from aind_metadata_mapper.models import EnrichmentSettings
from aind_metadata_mapper.utils import get_subject

ENRICHED_FIELDS = (
    "subject_details.animal_weight_prior",
    "subject_details.animal_weight_post",
    "ethics_review_id",
)


def _get_field(acquisition: dict, dotted_path: str):
    """Return a nested acquisition value by dotted path, or None if any level is missing."""
    value = acquisition
    for key in dotted_path.split("."):
        value = (value or {}).get(key)
    return value


def main() -> None:
    """Load an acquisition, enrich it, and print the enriched fields before and after."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject-id", default="864846")
    parser.add_argument("--acquisition-start")
    parser.add_argument("--acquisition-file", type=argparse.FileType("r"))
    parser.add_argument("--metadata-service-url", default="http://aind-metadata-service")
    parser.add_argument("--replace-existing", action="store_true")
    args = parser.parse_args()

    if args.acquisition_file:
        with args.acquisition_file as acquisition_file:
            acquisition = json.load(acquisition_file)
    else:
        acquisition = {
            "subject_id": "864846",
            "acquisition_start_time": "2026-08-07T16:03:19.408769Z",
            "acquisition_end_time": "2026-08-07T17:27:29.118057Z",
            "subject_details": {"mouse_platform_name": "wheel"},
        }
    if args.acquisition_start:
        acquisition["acquisition_start_time"] = args.acquisition_start
    subject_id = str(acquisition.get("subject_id", args.subject_id) if args.acquisition_file else args.subject_id)

    subject = get_subject(subject_id, base_url=urljoin(args.metadata_service_url, "/api/v2/subject/"))
    if subject is None:
        raise SystemExit(f"Could not fetch subject {subject_id} from {args.metadata_service_url}.")

    before = copy.deepcopy(acquisition)
    enriched = enrich_acquisition(
        acquisition=acquisition,
        subject=subject,
        subject_id=subject_id,
        settings=EnrichmentSettings(replace_existing=args.replace_existing),
        metadata_service_url=args.metadata_service_url,
    )
    result = {
        "subject_id": subject_id,
        "acquisition_start_time": acquisition.get("acquisition_start_time"),
        "before": {field: _get_field(before, field) for field in ENRICHED_FIELDS},
        "after": {field: _get_field(enriched, field) for field in ENRICHED_FIELDS},
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
