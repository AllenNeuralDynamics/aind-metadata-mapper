# flake8: noqa: C901, F541
"""Integration test script for GatherMetadataJob functionality."""

import logging
import shutil
import tempfile
from pathlib import Path

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings

logging.basicConfig(level=logging.INFO)

source_metadata_path = Path(__file__).parent.parent / "tests" / "resources" / "v2_metadata"

temp_dir = tempfile.mkdtemp(prefix="integration_test_")
temp_metadata_path = Path(temp_dir)

try:
    print("\n" + "=" * 80)
    print("INTEGRATION TEST: GatherMetadataJob")
    print("=" * 80)
    print(f"Temporary directory: {temp_metadata_path}")
    print()

    print("Copying acquisition.json and instrument.json to temp directory...")
    shutil.copy(source_metadata_path / "acquisition.json", temp_metadata_path / "acquisition.json")
    shutil.copy(source_metadata_path / "instrument.json", temp_metadata_path / "instrument.json")
    print("✓ Files copied")

    settings = JobSettings(
        metadata_dir=str(temp_metadata_path),
        subject_id="804670",
        project_name="Learning mFISH-V1omFISH",
        modalities=[Modality.POPHYS, Modality.BEHAVIOR_VIDEOS, Modality.BEHAVIOR],
    )

    job = GatherMetadataJob(settings=settings)

    print("\n" + "=" * 80)
    print("STEP 1: Testing individual metadata retrieval")
    print("=" * 80)

    print("\nLoading acquisition from local file...")
    acquisition = job.get_acquisition()
    if acquisition:
        print(f"✓ Acquisition loaded for subject: {acquisition.get('subject_id')}")
        print(f"  Start time: {acquisition.get('acquisition_start_time')}")
        print(f"  Instrument ID: {acquisition.get('instrument_id')}")
    else:
        print("✗ Failed to load acquisition")

    print("\nLoading instrument from local file...")
    instrument = job.get_instrument()
    if instrument:
        print(f"✓ Instrument loaded: {instrument.get('instrument_id')}")
        print(f"  Modification date: {instrument.get('modification_date')}")
    else:
        print("✗ Failed to load instrument")

    print("\nFetching subject from metadata service...")
    subject = job.get_subject()
    if subject:
        print(f"✓ Subject fetched: {subject.get('subject_id')}")
        print(f"  Species: {subject.get('species', {}).get('name')}")
        print(f"  Sex: {subject.get('sex')}")
        print(f"  Genotype: {subject.get('genotype')}")
    else:
        print("✗ Failed to fetch subject")

    print("\nFetching procedures from metadata service...")
    procedures = job.get_procedures()
    if procedures:
        print(f"✓ Procedures fetched for subject: {procedures.get('subject_id')}")
        if procedures.get("subject_procedures"):
            print(f"  Number of procedures: {len(procedures['subject_procedures'])}")
            for i, proc in enumerate(procedures["subject_procedures"][:3], 1):
                print(f"    {i}. {proc.get('object_type', proc.get('procedure_type', 'Unknown'))}")
    else:
        print("✗ Failed to fetch procedures")

    print("\nFetching funding from metadata service...")
    funding_source, investigators = job.get_funding()
    if funding_source:
        print(f"✓ Funding fetched: {len(funding_source)} funding source(s)")
        for fund in funding_source:
            print(f"  Funder: {fund.get('funder', {}).get('name', 'Unknown')}")
    else:
        print("✗ No funding information found")

    if investigators:
        print(f"✓ Investigators fetched: {len(investigators)} investigator(s)")
        for inv in investigators:
            print(f"  - {inv.get('name')}")
    else:
        print("✗ No investigators found")

    print("\n" + "=" * 80)
    print("STEP 2: Running full run_job() workflow")
    print("=" * 80)
    print("This will fetch all metadata, validate, and write JSON files...")
    print()

    job.run_job()

    print("\n" + "=" * 80)
    print("STEP 3: Checking generated files")
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
        file_path = temp_metadata_path / file_name
        if file_path.exists():
            size = file_path.stat().st_size
            print(f"✓ {file_name} ({size:,} bytes)")
            generated_files.append(file_name)
        else:
            print(f"✗ {file_name} (not found)")

    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Individual retrieval tests:")
    print(f"  Acquisition: {'✓' if acquisition else '✗'}")
    print(f"  Instrument: {'✓' if instrument else '✗'}")
    print(f"  Subject: {'✓' if subject else '✗'}")
    print(f"  Procedures: {'✓' if procedures else '✗'}")
    print(f"  Funding: {'✓' if funding_source else '✗'}")
    print(f"  Investigators: {'✓' if investigators else '✗'}")
    print()
    print(f"run_job() execution:")
    print(f"  Files generated: {len(generated_files)}/{len(expected_files)}")
    print(f"  Status: {'✓ SUCCESS' if len(generated_files) == len(expected_files) else '✗ INCOMPLETE'}")
    print("=" * 80 + "\n")

finally:
    print(f"Cleaning up temporary directory: {temp_metadata_path}")
    shutil.rmtree(temp_dir)
    print("✓ Cleanup complete\n")
