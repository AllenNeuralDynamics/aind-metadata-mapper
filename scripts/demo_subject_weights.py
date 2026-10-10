"""Fetch a subject and demonstrate acquisition-day weight enrichment."""

import argparse
import json

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import DataDescriptionSettings, JobSettings


def main() -> None:
    """Fetch a subject and print weights selected around the acquisition midpoint."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject-id", default="864846")
    parser.add_argument("--acquisition-start", default="2026-08-07T00:18:00")
    parser.add_argument(
        "--acquisition-end",
        help="Acquisition end time in ISO format; required to classify a post weight.",
    )
    args = parser.parse_args()

    settings = JobSettings(
        output_dir=".",
        subject_id=args.subject_id,
        data_description_settings=DataDescriptionSettings(
            project_name="Weight records demo",
            modalities=[Modality.BEHAVIOR],
        ),
    )
    job = GatherMetadataJob(settings)
    subject = job.get_subject(subject_id=args.subject_id)
    if subject is None:
        raise SystemExit(f"Could not fetch subject {args.subject_id} from {settings.metadata_service_url}.")

    acquisition = {"acquisition_start_time": args.acquisition_start}
    if args.acquisition_end:
        acquisition["acquisition_end_time"] = args.acquisition_end

    enriched_subject = job.add_subject_weights(subject, acquisition, args.subject_id)
    subject_details = enriched_subject["subject_details"]
    result = {
        "subject_id": args.subject_id,
        "acquisition_start_time": args.acquisition_start,
        "acquisition_end_time": args.acquisition_end,
        "pre_weight": subject_details.get("pre_weight"),
        "post_weight": subject_details.get("post_weight"),
    }
    print(json.dumps(result, indent=2))
    if not args.acquisition_end:
        print("Pass --acquisition-end with the actual end time to classify a post weight.")


if __name__ == "__main__":
    main()
