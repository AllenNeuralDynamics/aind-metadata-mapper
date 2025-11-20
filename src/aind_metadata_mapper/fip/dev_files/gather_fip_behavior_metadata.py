#!/usr/bin/env python3
"""Gather and combine FIP and behavior metadata using GatherMetadataJob.

This script demonstrates using the actual metadata gathering system to combine
FIP and behavior acquisitions, rather than manually combining them.

Environment Setup:
    conda create -n fip-mapper python=3.11
    conda activate fip-mapper
    pip install -e .

Usage:
    cd src/aind_metadata_mapper/fip/dev_files
    python gather_fip_behavior_metadata.py /path/to/data/directory

Example with real data:
    cd src/aind_metadata_mapper/fip/dev_files
    python gather_fip_behavior_metadata.py /allen/aind/stage/vr-foraging/data/804434/804434_2025-11-14T010241Z
"""

import argparse
from pathlib import Path

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings


def main():
    """Gather and combine FIP and behavior metadata."""
    parser = argparse.ArgumentParser(description="Gather FIP and behavior metadata using GatherMetadataJob")
    parser.add_argument("data_directory", help="Path to data directory with fip.json and acquisition.json")
    args = parser.parse_args()

    data_dir = Path(args.data_directory)

    # Create output directory in dev_files (local to repo)
    output_dir = Path(__file__).parent / "gathered_output"
    output_dir.mkdir(exist_ok=True)

    # Extract subject_id and project from directory structure
    # Typical path: /allen/aind/stage/vr-foraging/data/804434/804434_2025-11-14T010241Z
    subject_id = data_dir.parts[-2]  # Gets "804434"
    project_name = "vr-foraging"  # Could parse from path if needed

    # Create JobSettings with local output directory
    settings = JobSettings(
        input_source=str(data_dir),
        metadata_dir=str(output_dir),
        subject_id=subject_id,
        project_name=project_name,
        modalities=[Modality.FIB, Modality.BEHAVIOR],
    )

    print(f"Reading metadata from: {data_dir}")
    print(f"  Subject ID: {subject_id}")
    print(f"  Project: {project_name}")
    print(f"  Modalities: {[m.abbreviation for m in settings.modalities]}")

    # Create and run the job
    job = GatherMetadataJob(settings=settings)
    job.run_job()

    print(f"\nMetadata files written to: {output_dir}")


if __name__ == "__main__":
    main()
