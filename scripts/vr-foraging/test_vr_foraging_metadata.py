"""Integration test for VR Foraging metadata collection."""

import logging
import shutil
import tempfile
from pathlib import Path

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings

logging.basicConfig(level=logging.INFO)

source_metadata_path = Path(__file__).parent

output_subfolder = Path(tempfile.mkdtemp(prefix="vr_foraging_test_"))

try:
    print("\n" + "=" * 80)
    print("INTEGRATION TEST: VR Foraging Metadata")
    print("=" * 80)
    print(f"Source metadata: {source_metadata_path}")
    print(f"Output directory: {output_subfolder}")
    print()

    settings = JobSettings(
        input_metadata_path=str(source_metadata_path),
        output_metadata_path=str(output_subfolder),
        subject_id="828422",
        project_name="Cognitive flexibility in patch foraging",
        modalities=[Modality.BEHAVIOR, Modality.BEHAVIOR_VIDEOS],
    )

    job = GatherMetadataJob(settings=settings)

    print("=" * 80)
    print("STEP 1: Testing individual metadata retrieval")
    print("=" * 80)

    print("\nLoading acquisition from local file...")
    acquisition = job.get_acquisition()
    if acquisition:
        print(f"✓ Acquisition loaded for subject: {acquisition.get('subject_id')}")
        print(f"  Start time: {acquisition.get('acquisition_start_time')}")
        print(f"  Instrument ID: {acquisition.get('instrument_id')}")
        print(f"  Acquisition type: {acquisition.get('acquisition_type')}")
    else:
        print("✗ Failed to load acquisition")

    print("\nLoading instrument from local file...")
    instrument = job.get_instrument()
    if instrument:
        print(f"✓ Instrument loaded: {instrument.get('instrument_id')}")
        print(f"  Modification date: {instrument.get('modification_date')}")
        modalities = instrument.get('modalities', [])
        print(f"  Modalities: {len(modalities)}")
        for mod in modalities:
            print(f"    - {mod.get('name')}")
    else:
        print("✗ Failed to load instrument")

    print("\n" + "=" * 80)
    print("STEP 2: Running full run_job() workflow")
    print("=" * 80)
    print("This will fetch all metadata, validate, and write JSON files...")
    print()

    job.run_job()

    print("\n" + "=" * 80)
    print("STEP 3: Checking generated files in output folder")
    print("=" * 80)

    expected_files = [
        "data_description.json",
        "subject.json",
        "procedures.json",
        "acquisition.json",
        "instrument.json",
    ]

    generated_files = []
    for file_name in expected_files:
        file_path = output_subfolder / file_name
        if file_path.exists():
            size = file_path.stat().st_size
            print(f"✓ {file_name} ({size:,} bytes)")
            generated_files.append(file_name)
        else:
            print(f"✗ {file_name} (not found)")

    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print("Individual retrieval tests:")
    print(f"  Acquisition: {'✓' if acquisition else '✗'}")
    print(f"  Instrument: {'✓' if instrument else '✗'}")
    print()
    print("run_job() execution:")
    print(f"  Files generated: {len(generated_files)}/{len(expected_files)}")
    print(f"  Status: {'✓ SUCCESS' if len(generated_files) == len(expected_files) else '✗ INCOMPLETE'}")
    print("=" * 80 + "\n")

finally:
    pass
    # print(f"Cleaning up output directory: {output_subfolder}")
    # shutil.rmtree(output_subfolder, ignore_errors=True)
    # print("✓ Cleanup complete\n")
