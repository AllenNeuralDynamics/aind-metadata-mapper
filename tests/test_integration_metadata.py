"""Integration tests for metadata gathering using real resource files"""

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import Processing
from aind_data_schema.core.quality_control import QualityControl
from aind_data_schema.core.subject import Subject
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
METADATA_SERVICE_DIR = TEST_DIR / "resources" / "metadata_service"
V2_METADATA_DIR = TEST_DIR / "resources" / "v2_metadata"


class TestIntegrationMetadata(unittest.TestCase):
    """Integration tests using real metadata resource files"""

    @patch("os.makedirs")
    def setUp(self, mock_makedirs):
        """Set up test fixtures"""
        self.test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="804670",
            project_name="Visual Behavior",
            modalities=[Modality.BEHAVIOR, Modality.ECEPHYS],
            metadata_service_url="http://test-service.com",
            acquisition_start_time=datetime(2025, 9, 17, 10, 26, 0, tzinfo=timezone.utc),
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
            """Mock file existence check"""
            return file_name in v2_files or file_name == "data_description.json"

        mock_file_exists.side_effect = mock_exists

        # Mock file reading
        def mock_read_file(file_name):
            """Mock file reading"""
            if file_name in v2_files:
                return v2_files[file_name]
            elif file_name == "data_description.json":
                # Created by build_data_description, won't be called
                return v2_files["data_description.json"]
            else:
                raise FileNotFoundError(f"File {file_name} not found")

        mock_get_file.side_effect = mock_read_file

        # Mock file writing methods to avoid actual file writing
        with patch.object(self.job, "_write_json_file") as mock_write:
            # Run the job
            try:
                self.job.run_job()

                # Verify that files were "written" (mocked)
                written_files = [call[0][0] for call in mock_write.call_args_list]

                # Check that individual metadata files were written through _write_json_file
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
        mock_file_exists.return_value = False
        acquisition_data = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")

        def mock_read_file(file_name):
            if file_name == "acquisition.json":
                return acquisition_data
            else:
                raise FileNotFoundError(f"File {file_name} not found")

        mock_get_file.side_effect = mock_read_file

        funding_data = self._load_resource_file(METADATA_SERVICE_DIR, "funding_response.json")

        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = funding_data
            mock_get.return_value = mock_response

            acquisition_start_time = acquisition_data["acquisition_start_time"]
            subject_id = acquisition_data["subject_id"]
            result = self.job.build_data_description(
                acquisition_start_time=acquisition_start_time,
                subject_id=subject_id
            )

        self.assertIn("name", result)
        self.assertIn("creation_time", result)

        expected_name = "804670_2025-09-17_10-26-00"
        self.assertEqual(result["name"], expected_name)

        from datetime import datetime

        now = datetime.now()
        creation_time_str = result["creation_time"]
        self.assertIn(str(now.year), creation_time_str)
        self.assertIn(f"{now.month:02d}", creation_time_str)

    def test_validate_and_create_metadata_with_raise_if_invalid_false(self):
        """Test validate_and_create_metadata with raise_if_invalid=False (default) - prints errors but not raise"""
        # Create core metadata with invalid data that will cause ValidationError
        core_metadata = {
            "data_description": {
                "name": "test_dataset",
                "creation_time": "2023-01-01T12:00:00",
                "institution": {"name": "Allen Institute for Neural Dynamics"},
                "data_level": "raw",
                "modalities": ["Behavior videos"],
                "project_name": "Test Project",
                "funding_source": [{"funder": "Test Funder"}],
                "investigators": [{"name": "Test Investigator"}],
                "subject_id": "invalid_subject_id",  # This will cause validation issues
            },
            "subject": {
                "subject_id": "different_subject_id",  # Mismatch with data_description will cause validation error
                "sex": "Invalid Sex Value",  # Invalid enum value
                "date_of_birth": "invalid-date-format",  # Invalid date format
            },
            "acquisition": {
                "subject_id": "yet_another_subject_id",  # Another mismatch
                "acquisition_start_time": "invalid-datetime",  # Invalid datetime
            },
        }

        # Capture logging output to verify error messages are logged
        from unittest.mock import patch

        with patch("logging.warning") as mock_warning:
            result = self.job.validate_and_create_metadata(core_metadata)

            self.assertIsNotNone(result)

            mock_warning.assert_called()
            warning_calls = [call[0][0] for call in mock_warning.call_args_list]
            self.assertTrue(any("Validation Errors Found:" in call for call in warning_calls))

    def test_validate_and_create_metadata_with_raise_if_invalid_true(self):
        """Test validate_and_create_metadata with raise_if_invalid=True - should raise ValidationError"""
        with patch("os.makedirs"):
            strict_settings = JobSettings(
                metadata_dir="/test/metadata",
                output_dir="/test/output",
                subject_id="804670",
                project_name="Visual Behavior",
                modalities=[Modality.BEHAVIOR, Modality.ECEPHYS],
                metadata_service_url="http://test-service.com",
                raise_if_invalid=True,
                acquisition_start_time=datetime(2025, 9, 17, 10, 26, 0),
            )
            strict_job = GatherMetadataJob(settings=strict_settings)

        # Create core metadata with invalid data that will cause ValidationError
        core_metadata = {
            "data_description": {
                "name": "test_dataset",
                "creation_time": "2023-01-01T12:00:00",
                "institution": {"name": "Allen Institute for Neural Dynamics"},
                "data_level": "raw",
                "modalities": ["Behavior videos"],
                "project_name": "Test Project",
                "funding_source": [{"funder": "Test Funder"}],
                "investigators": [{"name": "Test Investigator"}],
                "subject_id": "invalid_subject_id",  # This will cause validation issues
            },
            "subject": {
                "subject_id": "different_subject_id",  # Mismatch will cause validation error
                "sex": "Invalid Sex Value",  # Invalid enum value
                "date_of_birth": "invalid-date-format",  # Invalid date format
            },
            "acquisition": {
                "subject_id": "yet_another_subject_id",  # Another mismatch
                "acquisition_start_time": "invalid-datetime",  # Invalid datetime
            },
        }

        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            strict_job.validate_and_create_metadata(core_metadata)

    def test_validate_and_create_metadata_success(self):
        """Test validate_and_create_metadata with valid real metadata resources"""
        data_description = self._load_resource_file(V2_METADATA_DIR, "data_description.json")
        subject = self._load_resource_file(V2_METADATA_DIR, "subject.json")
        acquisition = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        procedures = self._load_resource_file(V2_METADATA_DIR, "procedures.json")
        instrument = self._load_resource_file(V2_METADATA_DIR, "instrument.json")
        processing = self._load_resource_file(V2_METADATA_DIR, "processing.json")
        quality_control = self._load_resource_file(V2_METADATA_DIR, "quality_control.json")

        core_metadata = {
            "data_description": data_description,
            "subject": subject,
            "acquisition": acquisition,
            "procedures": procedures,
            "instrument": instrument,
            "processing": processing,
            "quality_control": quality_control,
        }

        metadata = self.job.validate_and_create_metadata(core_metadata)

        self.assertIsInstance(metadata, Metadata)

    def test_validate_and_create_metadata_failure_fallback(self):
        """Test validate_and_create_metadata fallback using real metadata with invalid field"""
        data_description = self._load_resource_file(V2_METADATA_DIR, "data_description.json")
        subject = self._load_resource_file(V2_METADATA_DIR, "subject.json")
        acquisition = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        procedures = self._load_resource_file(V2_METADATA_DIR, "procedures.json")
        instrument = self._load_resource_file(V2_METADATA_DIR, "instrument.json")
        processing = self._load_resource_file(V2_METADATA_DIR, "processing.json")
        quality_control = self._load_resource_file(V2_METADATA_DIR, "quality_control.json")

        del data_description["subject_id"]

        core_metadata = {
            "data_description": data_description,
            "subject": subject,
            "acquisition": acquisition,
            "procedures": procedures,
            "instrument": instrument,
            "processing": processing,
            "quality_control": quality_control,
        }

        metadata = self.job.validate_and_create_metadata(core_metadata)

        self.assertIsInstance(metadata, dict)

    def test_validate_and_get_subject_id_from_real_acquisition(self):
        """Test _validate_and_get_subject_id with real acquisition metadata"""
        acquisition = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        
        # Test that it extracts subject_id correctly
        subject_id = self.job._validate_and_get_subject_id(acquisition)
        self.assertEqual(subject_id, "804670")

    def test_validate_acquisition_start_time_with_real_data(self):
        """Test _validate_acquisition_start_time with real acquisition metadata"""
        acquisition = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        acq_start_time_str = acquisition["acquisition_start_time"]
        
        # Test that it validates correctly
        result = self.job._validate_acquisition_start_time(acq_start_time_str)
        self.assertIsInstance(result, str)
        self.assertIn("2025-09-17", result)

    def test_validate_and_get_subject_id_mismatch_with_real_data(self):
        """Test _validate_and_get_subject_id raises error with mismatched subject_id"""
        acquisition = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")
        acq_start_time_str = acquisition["acquisition_start_time"].replace("Z", "+00:00")
        acq_start_time = datetime.fromisoformat(acq_start_time_str)
        
        with patch("os.makedirs"):
            test_settings = JobSettings(
                metadata_dir="/test/metadata",
                output_dir="/test/output",
                subject_id="123456",  # Different from acquisition (804670)
                project_name="Visual Behavior",
                modalities=[Modality.BEHAVIOR, Modality.ECEPHYS],
                metadata_service_url="http://test-service.com",
                acquisition_start_time=acq_start_time,
                raise_if_invalid=True,
            )
            test_job = GatherMetadataJob(settings=test_settings)

        # Should raise ValueError due to subject_id mismatch
        with self.assertRaises(ValueError) as context:
            test_job._validate_and_get_subject_id(acquisition)

        self.assertIn("subject_id from acquisition metadata", str(context.exception))
        self.assertIn("does not match", str(context.exception))

    def test_run_job_pulls_subject_id_and_acquisition_start_time_from_file(self):
        """Test that subject_id and acquisition_start_time are pulled from acquisition.json when None in settings"""
        with patch("os.makedirs"):
            test_settings = JobSettings(
                metadata_dir="/test/metadata",
                output_dir="/test/output",
                subject_id=None,
                acquisition_start_time=None,
                project_name="Visual Behavior",
                modalities=[Modality.BEHAVIOR],
            )
            test_job = GatherMetadataJob(settings=test_settings)

        acquisition_data = self._load_resource_file(V2_METADATA_DIR, "acquisition.json")

        with patch.object(test_job, "get_acquisition", return_value=acquisition_data):
            with patch.object(test_job, "build_data_description", return_value={"name": "test"}) as mock_build:
                with patch.object(test_job, "add_core_metadata", return_value={}) as mock_add:
                    with patch.object(test_job, "validate_and_create_metadata"):
                        with patch.object(test_job, "_write_json_file"):
                            test_job.run_job()

        mock_build.assert_called_once_with(
            acquisition_start_time=acquisition_data["acquisition_start_time"],
            subject_id="804670"
        )
        mock_add.assert_called_once()
        call_args = mock_add.call_args
        self.assertEqual(call_args.kwargs["subject_id"], "804670")


if __name__ == "__main__":
    unittest.main()
