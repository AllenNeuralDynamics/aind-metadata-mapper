#!/usr/bin/env python3
"""Gather and combine FIP and behavior metadata using GatherMetadataJob.

This script demonstrates using the actual metadata gathering system to combine
FIP and behavior acquisitions, rather than manually combining them.

Production Workflow:
    1. On the rig: Bruno (behavior system) creates acquisition.json (behavior acquisition)
    2. On the rig: FIP extractor creates fip.json
    3. Watchdog moves fip.json from rig to VAST directory
    4. Watchdog moves acquisition.json from rig to VAST directory and renames it to
       acquisition_behavior.json (so GatherMetadataJob can identify it by modality
       and merge it with other acquisition_*.json files)
    5. GatherMetadataJob runs with metadata_dir and output_dir both pointing to the
       VAST directory, finds fip.json, runs FIP mapper to create acquisition_fip.json,
       then merges all acquisition_*.json files into a single acquisition.json

This Script (Development/Testing):
    This script simulates steps 3-5 above. The output_dir (gathered_output folder)
    simulates the VAST directory where watchdog has moved and renamed files. The script:
    1. Copies fip.json from input directory to output_dir (simulating watchdog step 3)
    2. Copies acquisition.json from input directory to output_dir and renames it to
       acquisition_behavior.json (simulating watchdog step 4)
    3. Runs GatherMetadataJob pointing at output_dir (simulating step 5)

Prerequisites:
    The data_directory argument should point to the VAST location (where watchdog
    has moved the files). It must contain:
    - fip.json: FIP metadata file (created by FIP extractor, moved by watchdog)
    - acquisition.json: Behavior acquisition metadata file (created by Bruno/behavior
      system, moved by watchdog, but NOT yet renamed)

    Note: In production, the watchdog would rename acquisition.json to
    acquisition_behavior.json before GatherMetadataJob runs. This script simulates
    that renaming step.

The script works as follows:
1. Simulate watchdog: Copy acquisition.json from VAST to output_dir and rename it
   to acquisition_behavior.json (GatherMetadataJob expects files in format
   acquisition_{modality}.json).
2. Create temp metadata_dir with only fip.json (no acquisition.json) so mappers run.
3. GatherMetadataJob runs the FIP mapper, which transforms fip.json →
   acquisition_fip.json in output_dir.
4. GatherMetadataJob merges all acquisition_*.json files from output_dir into
   a single acquisition.json.

Environment Setup:
    conda create -n fip-mapper python=3.11
    conda activate fip-mapper
    pip install -e .

Usage:
    cd src/aind_metadata_mapper/fip/dev_files
    python gather_fip_behavior_metadata.py /path/to/vast/directory

Example with real data (VAST location):
    cd src/aind_metadata_mapper/fip/dev_files
    python gather_fip_behavior_metadata.py /allen/aind/stage/vr-foraging/data/804434/804434_2025-11-14T010241Z
"""

import argparse
import shutil
from pathlib import Path

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings


def main():
    """Gather and combine FIP and behavior metadata."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "data_directory",
        help="Path to VAST directory (where watchdog moved files) containing fip.json and acquisition.json",
    )
    args = parser.parse_args()

    # Input directory: where files are located (simulates rig directory before watchdog moves them)
    input_dir = Path(args.data_directory)
    # Output directory: simulates VAST directory where watchdog has moved and renamed files
    # In production, this would be the actual VAST location
    output_dir = Path(__file__).parent / "gathered_output"
    output_dir.mkdir(exist_ok=True)

    # Step 1: Simulate watchdog step 3 - copy fip.json to output_dir (VAST location)
    fip_json = input_dir / "fip.json"
    if fip_json.exists():
        output_fip = output_dir / "fip.json"
        if output_fip.exists():
            output_fip.unlink()  # Remove existing file to avoid permission issues
        shutil.copy(fip_json, output_fip)
        output_fip.chmod(0o644)  # Ensure read permissions
    else:
        print(f"Warning: {fip_json} not found in {input_dir}. FIP mapper will not run.")

    # Step 2: Simulate watchdog step 4 - copy acquisition.json to output_dir and rename to
    # acquisition_behavior.json. In production, watchdog would do this renaming so
    # GatherMetadataJob can identify it by modality (files must be in format
    # acquisition_{modality}.json).
    behavior_acq = input_dir / Acquisition.default_filename()
    if behavior_acq.exists():
        behavior_output = output_dir / "acquisition_behavior.json"
        if behavior_output.exists():
            behavior_output.unlink()  # Remove existing file to avoid permission issues
        shutil.copy(behavior_acq, behavior_output)
        behavior_output.chmod(0o644)  # Ensure read permissions
    else:
        print(f"Warning: {behavior_acq} not found in {input_dir}. Behavior metadata will not be included.")

    # Step 3: Simulate step 5 - run GatherMetadataJob pointing at output_dir (VAST location)
    # In production, metadata_dir and output_dir would both be the VAST directory.
    # GatherMetadataJob will:
    # - Find fip.json in metadata_dir, run FIP mapper to create acquisition_fip.json in output_dir
    # - Find acquisition_behavior.json in output_dir
    # - Merge all acquisition_*.json files into a single acquisition.json
    settings = JobSettings(
        metadata_dir=str(output_dir),  # Simulates VAST directory
        output_dir=str(output_dir),  # Simulates VAST directory (same location in production)
        subject_id=input_dir.parts[-2],
        project_name="Cognitive flexibility in patch foraging",
        modalities=[Modality.FIB, Modality.BEHAVIOR],
    )

    job = GatherMetadataJob(settings=settings)

    # Workaround: _merge_models doesn't use mode="json" so datetime objects aren't serialized
    # (This ensures datetime objects are properly serialized to JSON strings)
    original_merge = job._merge_models

    def merge_with_json_mode(model_class, models):
        result = original_merge(model_class, models)
        return model_class.model_validate(result).model_dump(mode="json") if result else {}

    job._merge_models = merge_with_json_mode

    job.run_job()

    print(f"\nMetadata files written to: {output_dir}")


if __name__ == "__main__":
    main()
