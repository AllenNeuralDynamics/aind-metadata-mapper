"""Integration tests for metadata gathering using real resource files"""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Dict, Any

from aind_data_schema.core.metadata import Metadata
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.subject import Subject
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.processing import Processing
from aind_data_schema.core.quality_control import QualityControl
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
METADATA_SERVICE_DIR = TEST_DIR / "resources" / "metadata_service"
V2_METADATA_DIR = TEST_DIR / "resources" / "v2_metadata"


class TestIntegrationMetadata(unittest.TestCase):
    """Integration tests using real metadata resource files"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_settings = JobSettings(
            metadata_dir="/test/metadata",
            subject_id="804670",
            project_name="Visual Behavior",
            modalities=[Modality.BEHAVIOR, Modality.ECEPHYS],
            metadata_service_url="http://test-service.com",
        )
        self.job = GatherMetadataJob(settings=self.test_settings)

    def _load_resource_file(self, resource_dir: Path, filename: str) -> Dict[str, Any]:  # noqa: E501
        """Helper method to load JSON resource files"""
        file_path = resource_dir / filename
        with open(file_path, "r") as f:
            return json.load(f)

    def test_metadata_service_responses_structure(self):
        """Test that metadata service response files have expected structure"""
        # Test funding response structure
        funding_data = self._load_resource_file(METADATA_SERVICE_DIR, "funding_response.json")
        self.assertIsInstance(funding_data, list)
        self.assertGreater(len(funding_data), 0)

        first_funding = funding_data[0]
        self.assertEqual(first_funding["object_type"], "Funding information")
        self.assertIn("funder", first_funding)
        self.assertIn("investigators", first_funding)

        # Test subject response structure
        subject_data = self._load_resource_file(METADATA_SERVICE_DIR, "subject_response.json")
        self.assertEqual(subject_data["object_type"], "Subject")
        self.assertEqual(subject_data["subject_id"], "804670")
        self.assertIn("subject_details", subject_data)

        # Test procedures response structure
        procedures_data = self._load_resource_file(METADATA_SERVICE_DIR, "procedures_response.json")
        self.assertEqual(procedures_data["object_type"], "Procedures")
        self.assertEqual(procedures_data["subject_id"], "804670")
        self.assertIn("subject_procedures", procedures_data)

    def test_v2_metadata_files_structure(self):
        """Test that v2 metadata files have expected structure"""
        # Test data description
        data_desc = self._load_resource_file(V2_METADATA_DIR, "data_description.json")
        self.assertEqual(data_desc["object_type"], "Data description")
        self.assertEqual(data_desc["subject_id"], "804670")
        self.assertIn("funding_source", data_desc)
        self.assertIn("investigators", data_desc)

        # Test subject
        subject = self._load_resource_file(V2_METADATA_DIR, "subject.json")
        self.assertEqual(subject["object_type"], "Subject")
        self.assertEqual(subject["subject_id"], "804670")

        # Test acquisition
        acquisition = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        self.assertEqual(acquisition["object_type"], "Acquisition")
        self.assertEqual(acquisition["subject_id"], "804670")

        # Test procedures
        procedures = self._load_resource_file(V2_METADATA_DIR, "procedures.json")
        self.assertEqual(procedures["object_type"], "Procedures")
        self.assertEqual(procedures["subject_id"], "804670")

        # Test instrument
        instrument = self._load_resource_file(V2_METADATA_DIR, "instrument.json")
        self.assertEqual(instrument["object_type"], "Instrument")

        # Test processing
        processing = self._load_resource_file(V2_METADATA_DIR, "processing.json")
        self.assertEqual(processing["object_type"], "Processing")

        # Test quality control
        quality_control = self._load_resource_file(V2_METADATA_DIR, "quality_control.json")
        self.assertEqual(quality_control["object_type"], "Quality control")

    @patch("requests.get")
    def test_get_funding_with_real_response(self, mock_get):
        """Test get_funding method with real funding response data"""
        funding_data = self._load_resource_file(METADATA_SERVICE_DIR, "funding_response.json")

        # Mock API response - funding_data is already a list, use it directly
        mock_response = MagicMock()
        mock_response.status_code = 300  # Multiple results
        mock_response.json.return_value = {"data": funding_data}
        mock_get.return_value = mock_response

        # Call the method
        funding_source, investigators = self.job.get_funding()

        # Verify the request was made correctly
        expected_url = f"{self.test_settings.metadata_service_url}/api/v2/funding/" f"{self.test_settings.project_name}"
        mock_get.assert_called_once_with(expected_url)

        # Verify funding source structure
        self.assertIsInstance(funding_source, list)
        self.assertEqual(len(funding_source), 2)  # Based on the test data

        # Verify investigators are extracted and deduplicated
        self.assertIsInstance(investigators, list)
        self.assertGreater(len(investigators), 0)

        # Check that investigators are Person objects (dicts)
        for investigator in investigators:
            self.assertIsInstance(investigator, dict)
            self.assertEqual(investigator["object_type"], "Person")
            self.assertIn("name", investigator)

    @patch("requests.get")
    def test_get_subject_with_real_response(self, mock_get):
        """Test get_subject method with real subject response data"""
        subject_data = self._load_resource_file(METADATA_SERVICE_DIR, "subject_response.json")

        # Mock the API response - subject_data is already the subject object
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": subject_data}
        mock_get.return_value = mock_response

        # Call the method
        result = self.job.get_subject()

        # Verify the request was made correctly
        expected_url = f"{self.test_settings.metadata_service_url}/api/v2/subject/" f"{self.test_settings.subject_id}"
        mock_get.assert_called_once_with(expected_url)

        # Verify the result structure
        self.assertEqual(result, subject_data)
        self.assertEqual(result["subject_id"], "804670")
        self.assertEqual(result["object_type"], "Subject")

    @patch("requests.get")
    def test_get_procedures_with_real_response(self, mock_get):
        """Test get_procedures method with real procedures response data"""
        procedures_data = self._load_resource_file(METADATA_SERVICE_DIR, "procedures_response.json")

        # Mock the API response - procedures_data is already the procedures object
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": procedures_data}
        mock_get.return_value = mock_response

        # Call the method
        result = self.job.get_procedures()

        # Verify the request was made correctly
        expected_url = (
            f"{self.test_settings.metadata_service_url}/api/v2/procedures/" f"{self.test_settings.subject_id}"
        )
        mock_get.assert_called_once_with(expected_url)

        # Verify the result structure
        self.assertEqual(result, procedures_data)
        self.assertEqual(result["subject_id"], "804670")
        self.assertEqual(result["object_type"], "Procedures")

    def test_validate_data_description_schema(self):
        """Test DataDescription validation"""
        data_desc_data = self._load_resource_file(V2_METADATA_DIR, "data_description.json")
        data_desc = DataDescription.model_validate(data_desc_data)
        self.assertIsInstance(data_desc, DataDescription)
        self.assertEqual(data_desc.subject_id, "804670")

    def test_validate_subject_schema(self):
        """Test Subject validation"""
        subject_data = self._load_resource_file(V2_METADATA_DIR, "subject.json")
        subject = Subject.model_validate(subject_data)
        self.assertIsInstance(subject, Subject)
        self.assertEqual(subject.subject_id, "804670")

    def test_validate_acquisition_schema(self):
        """Test Acquisition validation"""
        acquisition_data = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        acquisition = Acquisition.model_validate(acquisition_data)
        self.assertIsInstance(acquisition, Acquisition)
        self.assertEqual(acquisition.subject_id, "804670")

    def test_validate_procedures_schema(self):
        """Test Procedures validation"""
        procedures_data = self._load_resource_file(V2_METADATA_DIR, "procedures.json")
        procedures = Procedures.model_validate(procedures_data)
        self.assertIsInstance(procedures, Procedures)
        self.assertEqual(procedures.subject_id, "804670")

    def test_validate_instrument_schema(self):
        """Test Instrument validation"""
        instrument_data = self._load_resource_file(V2_METADATA_DIR, "instrument.json")
        instrument = Instrument.model_validate(instrument_data)
        self.assertIsInstance(instrument, Instrument)

    def test_validate_processing_schema(self):
        """Test Processing validation"""
        processing_data = self._load_resource_file(V2_METADATA_DIR, "processing.json")
        processing = Processing.model_validate(processing_data)
        self.assertIsInstance(processing, Processing)

    def test_validate_quality_control_schema(self):
        """Test QualityControl validation"""
        qc_data = self._load_resource_file(V2_METADATA_DIR, "quality_control.json")
        quality_control = QualityControl.model_validate(qc_data)
        self.assertIsInstance(quality_control, QualityControl)

    def test_validate_complete_metadata_object(self):
        """Test that v2 metadata files create a valid Metadata object"""
        # Load all metadata files
        data_description = self._load_resource_file(V2_METADATA_DIR, "data_description.json")
        subject = self._load_resource_file(V2_METADATA_DIR, "subject.json")
        acquisition = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        procedures = self._load_resource_file(V2_METADATA_DIR, "procedures.json")
        instrument = self._load_resource_file(V2_METADATA_DIR, "instrument.json")
        processing = self._load_resource_file(V2_METADATA_DIR, "processing.json")
        quality_control = self._load_resource_file(V2_METADATA_DIR, "quality_control.json")

        # Create core metadata dictionary
        core_metadata = {
            "data_description": data_description,
            "subject": subject,
            "acquisition": acquisition,
            "procedures": procedures,
            "instrument": instrument,
            "processing": processing,
            "quality_control": quality_control,
        }

        # Test validation through the job's validate_and_create_metadata method
        metadata = self.job.validate_and_create_metadata(core_metadata)
        self.assertIsInstance(metadata, (Metadata, dict))

        # If it's a Metadata object, check its properties
        if isinstance(metadata, Metadata):
            self.assertEqual(metadata.name, data_description["name"])
            self.assertIsNotNone(metadata.subject)
            self.assertIsNotNone(metadata.data_description)
            self.assertIsNotNone(metadata.acquisition)
            self.assertIsNotNone(metadata.procedures)
            self.assertIsNotNone(metadata.instrument)
            self.assertIsNotNone(metadata.processing)
            self.assertIsNotNone(metadata.quality_control)

    @patch("aind_metadata_mapper.gather_metadata.GatherMetadataJob." + "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.GatherMetadataJob." + "_get_file_from_user_defined_directory")
    def test_run_job_with_all_local_files(self, mock_get_file, mock_file_exists):
        """Test run_job method using all v2 metadata files as local files"""
        # Load all resource files
        v2_files = {
            "data_description.json": self._load_resource_file(V2_METADATA_DIR, "data_description.json"),
            "subject.json": self._load_resource_file(V2_METADATA_DIR, "subject.json"),
            "acquisition.json": self._load_resource_file(V2_METADATA_DIR, "acquisition.json"),
            "procedures.json": self._load_resource_file(V2_METADATA_DIR, "procedures.json"),
            "instrument.json": self._load_resource_file(V2_METADATA_DIR, "instrument.json"),
            "processing.json": self._load_resource_file(V2_METADATA_DIR, "processing.json"),
            "quality_control.json": self._load_resource_file(V2_METADATA_DIR, "quality_control.json"),
        }

        # Mock file existence - return True for files that exist locally
        def mock_exists(file_name):
            return file_name in v2_files or file_name == "data_description.json"

        mock_file_exists.side_effect = mock_exists

        # Mock file reading
        def mock_read_file(file_name):
            if file_name in v2_files:
                return v2_files[file_name]
            elif file_name == "data_description.json":
                # Created by build_data_description, won't be called
                return v2_files["data_description.json"]
            else:
                raise FileNotFoundError(f"File {file_name} not found")

        mock_get_file.side_effect = mock_read_file

        # Mock file writing methods to avoid actual file writing
        with (
            patch.object(self.job, "_write_json_file") as mock_write,
            patch("aind_data_schema.core.metadata.Metadata." "write_standard_file"),
        ):
            # Run the job
            try:
                self.job.run_job()

                # Verify that files were "written" (mocked)
                written_files = [call[0][0] for call in mock_write.call_args_list]

                # Check that files were written through _write_json_file
                # Note: metadata.nd.json is written via write_standard_file
                expected_files = [
                    "data_description.json",
                    "subject.json",
                    "procedures.json",
                    "acquisition.json",
                    "instrument.json",
                    "processing.json",
                    "quality_control.json",
                ]

                for expected_file in expected_files:
                    self.assertIn(expected_file, written_files, f"Expected {expected_file} to be written")

            except Exception as e:
                self.fail(f"run_job with all local files failed: {e}")

    @patch("aind_metadata_mapper.gather_metadata.GatherMetadataJob." + "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.GatherMetadataJob." + "_get_file_from_user_defined_directory")
    def test_build_data_description_uses_acquisition_start_time_for_name(self, mock_get_file, mock_file_exists):
        """Test that build_data_description uses acquisition_start_time for DataDescription name formatting"""
        # Load the real acquisition data
        acquisition_data = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")

        # Mock that no data_description.json exists locally, but acquisition.json does
        def mock_exists(file_name):
            return file_name == "acquisition.json"

        mock_file_exists.side_effect = mock_exists

        # Mock reading acquisition file
        def mock_read_file(file_name):
            if file_name == "acquisition.json":
                return acquisition_data
            else:
                raise FileNotFoundError(f"File {file_name} not found")

        mock_get_file.side_effect = mock_read_file

        # Mock the funding API call to use real funding data
        funding_data = self._load_resource_file(METADATA_SERVICE_DIR, "funding_response.json")

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 300
            mock_response.json.return_value = {"data": funding_data}
            mock_get.return_value = mock_response

            result = self.job.build_data_description()

        # Verify the result has the expected structure
        self.assertIn("name", result)
        self.assertIn("creation_time", result)

        # The acquisition_start_time is "2025-09-17 10:26:00.474680-07:00"
        # The name field should use acquisition start time in format: subject_id_YYYY-MM-DD_HH-MM-SS
        expected_name = "804670_2025-09-17_10-26-00"
        self.assertEqual(result["name"], expected_name)

        # The creation_time should be current time (when metadata was created)
        # This verifies the metadata creation timestamp, not the acquisition time
        from datetime import datetime

        now = datetime.now()
        creation_time_str = result["creation_time"]
        # Should be today's date in the creation_time
        self.assertIn(str(now.year), creation_time_str)
        self.assertIn(f"{now.month:02d}", creation_time_str)


if __name__ == "__main__":
    unittest.main()
