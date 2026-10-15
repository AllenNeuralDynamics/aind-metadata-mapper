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
    parser.add_argument("--acquisition-start")
    parser.add_argument("--acquisition-file", type=argparse.FileType("r"))
    parser.add_argument(
        "--acquisition-end",
        help="Acquisition end time in ISO format; required to classify a post weight.",
    )
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
    if args.acquisition_end:
        acquisition["acquisition_end_time"] = args.acquisition_end
    subject_id = acquisition.get("subject_id", args.subject_id) if args.acquisition_file else args.subject_id

    settings = JobSettings(
        output_dir=".",
        subject_id=subject_id,
        data_description_settings=DataDescriptionSettings(
            project_name="Weight records demo",
            modalities=[Modality.BEHAVIOR],
        ),
    )
    job = GatherMetadataJob(settings)
    subject = job.get_subject(subject_id=subject_id)
    if subject is None:
        raise SystemExit(f"Could not fetch subject {subject_id} from {settings.metadata_service_url}.")

    job.add_subject_weights(subject, acquisition, subject_id)
    subject_details = acquisition["subject_details"]
    result = {
        "subject_id": subject_id,
        "acquisition_start_time": acquisition.get("acquisition_start_time"),
        "acquisition_end_time": acquisition.get("acquisition_end_time"),
        "animal_weight_prior": subject_details.get("animal_weight_prior"),
        "animal_weight_post": subject_details.get("animal_weight_post"),
    }
    print(json.dumps(result, indent=2))
    if not acquisition.get("acquisition_end_time"):
        print("Pass --acquisition-end with the actual end time to classify a post weight.")


if __name__ == "__main__":
    main()
