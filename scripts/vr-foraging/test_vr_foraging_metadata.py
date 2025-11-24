"""Integration test for VR Foraging metadata collection."""

from datetime import datetime
import json
import logging
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings

logging.basicConfig(level=logging.INFO)

# Set to False to use mock responses from tests/resources/ instead of the metadata service
USE_METADATA_SERVICE = False

source_metadata_path = Path(__file__).parent
tests_resources_path = Path(__file__).parent.parent.parent / "tests" / "resources"

output_subfolder = Path(tempfile.mkdtemp(prefix="vr_foraging_test_"))


def load_mock_response(filename: str):
    """Load a mock response from tests/resources/metadata_service/"""
    filepath = tests_resources_path / "metadata_service" / filename
    with open(filepath, "r") as f:
        return json.load(f)


def mock_requests_get(url):
    """Mock requests.get to return responses from local files"""
    mock_response = Mock()

    if "/subject/" in url:
        mock_response.status_code = 200
        mock_response.json.return_value = load_mock_response("subject_response.json")
    elif "/procedures/" in url:
        mock_response.status_code = 200
        mock_response.json.return_value = load_mock_response("procedures_response.json")
    elif "/funding/" in url:
        mock_response.status_code = 200
        mock_response.json.return_value = load_mock_response("funding_response.json")
    else:
        mock_response.status_code = 404
        mock_response.json.return_value = {}

    return mock_response


def run_test():
    """Run the actual test logic"""
    print("\n" + "=" * 80)
    print("INTEGRATION TEST: VR Foraging Metadata")
    print("=" * 80)
    print(f"Source metadata: {source_metadata_path}")
    print(f"Output directory: {output_subfolder}")
    print(f"Using metadata service: {USE_METADATA_SERVICE}")
    print()

    settings = JobSettings(
        metadata_dir=str(source_metadata_path),
        output_dir=str(output_subfolder),
        subject_id="828422",
        project_name="Cognitive flexibility in patch foraging",
        modalities=[Modality.BEHAVIOR, Modality.BEHAVIOR_VIDEOS],
        acquisition_start_time=datetime.fromisoformat("2025-11-13T17:38:37.079861+00:00"),
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
        modalities = instrument.get("modalities", [])
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


if __name__ == "__main__":
    try:
        if USE_METADATA_SERVICE:
            # Run test with actual metadata service
            run_test()
        else:
            # Run test with mocked responses from tests/resources/
            print("Using mock responses from tests/resources/metadata_service/")
            with patch("aind_metadata_mapper.gather_metadata.requests.get", side_effect=mock_requests_get):
                run_test()
    finally:
        pass
        # print(f"Cleaning up output directory: {output_subfolder}")
        # shutil.rmtree(output_subfolder, ignore_errors=True)
        # print("✓ Cleanup complete\n")
