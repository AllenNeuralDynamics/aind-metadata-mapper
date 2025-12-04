"""Tests gather_metadata module"""

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.metadata import Metadata
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from aind_metadata_mapper.gather_metadata import GatherMetadataJob, _metadata_service_helper
from aind_metadata_mapper.models import JobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))


class TestMetadataServiceHelper(unittest.TestCase):
    """Tests for _metadata_service_helper function"""

    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_status_200(self, mock_get):
        """Test helper with successful 200 status"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"test": "data", "value": 123}
        mock_get.return_value = mock_response

        result = _metadata_service_helper("http://test.com/api")

        self.assertEqual(result, {"test": "data", "value": 123})
        mock_get.assert_called_once_with("http://test.com/api")
        mock_response.json.assert_called_once()

    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_status_400(self, mock_get):
        """Test helper with 400 status (invalid object but still returned)"""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "invalid", "details": "bad request"}
        mock_get.return_value = mock_response

        result = _metadata_service_helper("http://test.com/api")

        self.assertEqual(result, {"error": "invalid", "details": "bad request"})
        mock_get.assert_called_once_with("http://test.com/api")

    @patch("aind_metadata_mapper.gather_metadata.logging.error")
    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_status_404(self, mock_get, mock_log_error):
        """Test helper with 404 status"""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        result = _metadata_service_helper("http://test.com/notfound")

        self.assertIsNone(result)
        mock_log_error.assert_called_once()
        self.assertIn("404", str(mock_log_error.call_args))

    @patch("aind_metadata_mapper.gather_metadata.logging.error")
    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_status_500(self, mock_get, mock_log_error):
        """Test helper with 500 status"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = _metadata_service_helper("http://test.com/error")

        self.assertIsNone(result)
        mock_log_error.assert_called_once()
        self.assertIn("500", str(mock_log_error.call_args))

    @patch("aind_metadata_mapper.gather_metadata.logging.error")
    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_request_exception(self, mock_get, mock_log_error):
        """Test helper when request raises exception"""
        mock_get.side_effect = Exception("Connection failed")

        result = _metadata_service_helper("http://test.com/timeout")

        self.assertIsNone(result)
        mock_log_error.assert_called_once()
        self.assertIn("Connection failed", str(mock_log_error.call_args))

    @patch("aind_metadata_mapper.gather_metadata.logging.error")
    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_json_decode_error(self, mock_get, mock_log_error):
        """Test helper when json() raises exception"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        result = _metadata_service_helper("http://test.com/invalid")

        self.assertIsNone(result)
        mock_log_error.assert_called_once()
        self.assertIn("Invalid JSON", str(mock_log_error.call_args))

    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_empty_response(self, mock_get):
        """Test helper with empty response data"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        result = _metadata_service_helper("http://test.com/empty")

        self.assertEqual(result, {})

    @patch("aind_metadata_mapper.gather_metadata.requests.get")
    def test_metadata_service_helper_complex_response(self, mock_get):
        """Test helper with complex nested response"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "nested": [1, 2, 3],
                "object": {"key": "value"},
            },
            "metadata": {
                "timestamp": "2023-01-01T00:00:00Z",
            },
        }
        mock_get.return_value = mock_response

        result = _metadata_service_helper("http://test.com/complex")

        self.assertIsNotNone(result)
        self.assertEqual(result["data"]["nested"], [1, 2, 3])
        self.assertEqual(result["data"]["object"]["key"], "value")


class TestGatherMetadataJob(unittest.TestCase):
    """Tests methods in GatherMetadataJob class"""

    @patch("os.makedirs")
    def setUp(self, mock_makedirs):
        """Set up test fixtures"""
        self.test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS, Modality.BEHAVIOR],
            metadata_service_url="http://test-service.com",
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        self.job = GatherMetadataJob(settings=self.test_settings)

    # Tests for file system helper methods
    @patch("os.path.isfile")
    def test_does_file_exist_in_user_defined_dir_true(self, mock_isfile):
        """Test _does_file_exist_in_user_defined_dir when file exists"""
        mock_isfile.return_value = True
        result = self.job._does_file_exist_in_user_defined_dir("test_file.json")
        self.assertTrue(result)
        mock_isfile.assert_called_once_with("/test/metadata/test_file.json")

    @patch("os.path.isfile")
    def test_does_file_exist_in_user_defined_dir_false(self, mock_isfile):
        """Test _does_file_exist_in_user_defined_dir when file doesn't exist"""
        mock_isfile.return_value = False
        result = self.job._does_file_exist_in_user_defined_dir("missing_file.json")
        self.assertFalse(result)
        mock_isfile.assert_called_once_with("/test/metadata/missing_file.json")

    @patch("os.makedirs")
    def test_does_file_exist_in_user_defined_dir_no_metadata_dir(self, mock_makedirs):
        """Test _does_file_exist_in_user_defined_dir when metadata_dir is None"""
        job_settings = JobSettings(
            metadata_dir=None,
            output_dir="/test/output",
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        job = GatherMetadataJob(settings=job_settings)
        result = job._does_file_exist_in_user_defined_dir("test_file.json")
        self.assertFalse(result)

    @patch("builtins.open", new_callable=mock_open, read_data='{"test": "data"}')
    @patch("json.load")
    def test_get_file_from_user_defined_directory(self, mock_json_load, mock_file):
        """Test _get_file_from_user_defined_directory"""
        mock_json_load.return_value = {"test": "data"}
        result = self.job._get_file_from_user_defined_directory("test_file.json")

        mock_file.assert_called_once_with("/test/metadata/test_file.json", "r")
        mock_json_load.assert_called_once()
        self.assertEqual(result, {"test": "data"})

    # Tests for get_funding method
    @patch("requests.get")
    @patch("os.makedirs")
    def test_get_funding_no_project_name(self, mock_makedirs, mock_get):
        """Test get_funding when no project name is provided"""
        job_no_project = GatherMetadataJob(
            JobSettings(
                metadata_dir="/test",
                output_dir="/test/output",
                subject_id="test_subject",
                project_name="",
                modalities=[Modality.ECEPHYS],
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
        )
        funding, investigators = job_no_project.get_funding()

        self.assertEqual(funding, [])
        self.assertEqual(investigators, [])
        mock_get.assert_not_called()

    @patch("requests.get")
    def test_get_funding_success_single_result(self, mock_get):
        """Test get_funding with successful single result"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "funder": "Test Funder",
                "award_number": "12345",
                "investigators": [
                    Person(name="John Doe").model_dump(),
                    Person(name="Jane Smith").model_dump(),
                ],
            }
        ]
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        expected_funding = [{"funder": "Test Funder", "award_number": "12345"}]
        self.assertEqual(funding, expected_funding)
        self.assertEqual(len(investigators), 2)
        self.assertIsInstance(investigators[0], dict)

    @patch("requests.get")
    def test_get_funding_success_multiple_results(self, mock_get):
        """Test get_funding with successful multiple results"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "funder": "Funder 1",
                "award_number": "111",
                "investigators": [{"name": "Alice"}],
            },
            {
                "funder": "Funder 2",
                "award_number": "222",
                "investigators": [{"name": "Bob"}, {"name": "Charlie"}],
            },
        ]
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        self.assertEqual(len(funding), 2)
        self.assertEqual(len(investigators), 3)

    @patch("requests.get")
    def test_get_funding_empty_investigators(self, mock_get):
        """Test get_funding with empty investigators"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "funder": "Test Funder",
                "award_number": "12345",
                "investigators": [],
            }
        ]
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        self.assertEqual(len(investigators), 0)

    @patch("requests.get")
    @patch("logging.error")
    def test_get_funding_http_error(self, mock_error, mock_get):
        """Test get_funding with HTTP error"""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        self.assertEqual(funding, [])
        self.assertEqual(investigators, [])
        mock_error.assert_called()

    @patch("requests.get")
    @patch("logging.error")
    def test_get_funding_request_exception(self, mock_error, mock_get):
        """Test get_funding with request exception"""
        mock_get.side_effect = Exception("Network error")

        funding, investigators = self.job.get_funding()

        self.assertEqual(funding, [])
        self.assertEqual(investigators, [])
        mock_error.assert_called()

    # Tests for _write_json_file method
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_write_json_file(self, mock_json_dump, mock_file):
        """Test _write_json_file method"""
        test_data = {"key": "value", "number": 123}

        self.job._write_json_file("output.json", test_data)

        mock_file.assert_called_once_with("/test/output/output.json", "w")
        mock_json_dump.assert_called_once_with(
            test_data,
            mock_file().__enter__(),
            indent=3,
            ensure_ascii=False,
            sort_keys=True,
        )

    # Tests for build_data_description method
    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "_get_file_from_user_defined_directory")
    def test_build_data_description_existing_file(self, mock_get_file, mock_file_exists):
        """Test build_data_description when file already exists"""
        mock_file_exists.return_value = True
        mock_get_file.return_value = {"existing": "data"}

        result = self.job.build_data_description(acquisition_start_time="2023-01-01T12:00:00")

        self.assertEqual(result, {"existing": "data"})
        mock_file_exists.assert_called_once_with(file_name="data_description.json")
        mock_get_file.assert_called_once_with(file_name="data_description.json")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "get_funding")
    @patch("aind_metadata_mapper.gather_metadata.datetime")
    def test_build_data_description_new_file(self, mock_datetime, mock_get_funding, mock_file_exists):
        """Test build_data_description when creating new file"""
        mock_file_exists.return_value = False
        mock_get_funding.return_value = (
            [{"funder": Organization.NIMH, "grant_number": "12345"}],
            [{"name": "Test Investigator"}],
        )
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = self.job.build_data_description(acquisition_start_time="2023-01-01T12:00:00")

        self.assertIn("creation_time", result)
        self.assertEqual(result["project_name"], "Test Project")
        self.assertEqual(len(result["modalities"]), 2)

    # Tests for get_subject method
    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("os.makedirs")
    def test_get_subject_no_subject_id(self, mock_makedirs, mock_file_exists):
        """Test get_subject when no subject_id is provided"""
        job_no_subject = GatherMetadataJob(
            JobSettings(
                metadata_dir="/test",
                output_dir="/test/output",
                subject_id="",
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
        )

        result = job_no_subject.get_subject()

        self.assertIsNone(result)
        mock_file_exists.assert_not_called()

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "_get_file_from_user_defined_directory")
    def test_get_subject_existing_file(self, mock_get_file, mock_file_exists):
        """Test get_subject when file already exists"""
        mock_file_exists.return_value = True
        mock_get_file.return_value = {"subject_id": "123456"}

        result = self.job.get_subject()

        self.assertEqual(result, {"subject_id": "123456"})
        mock_file_exists.assert_called_once_with(file_name="subject.json")
        mock_get_file.assert_called_once_with(file_name="subject.json")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_subject_api_success(self, mock_get, mock_file_exists):
        """Test get_subject when downloading from API successfully"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"subject_id": "123456"}
        mock_get.return_value = mock_response

        result = self.job.get_subject()

        self.assertEqual(result, {"subject_id": "123456"})
        mock_get.assert_called_once_with("http://test-service.com/api/v2/subject/123456")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_subject_api_404(self, mock_get, mock_file_exists):
        """Test get_subject when subject not found in API"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "Subject not found"}
        mock_get.return_value = mock_response

        result = self.job.get_subject()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_subject_api_other_error(self, mock_get, mock_file_exists):
        """Test get_subject when API returns other error"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.json.return_value = {"error": "Server error"}
        mock_get.return_value = mock_response

        result = self.job.get_subject()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    @patch("logging.error")
    def test_get_subject_api_exception(self, mock_log_error, mock_get, mock_file_exists):
        """Test get_subject when API request raises exception"""
        mock_file_exists.return_value = False
        mock_get.side_effect = Exception("Network error")

        result = self.job.get_subject()

        self.assertIsNone(result)
        mock_log_error.assert_called_once()

    # Tests for get_procedures method
    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("os.makedirs")
    def test_get_procedures_no_subject_id(self, mock_makedirs, mock_file_exists):
        """Test get_procedures when no subject_id is provided"""
        job_no_subject = GatherMetadataJob(
            JobSettings(
                metadata_dir="/test",
                output_dir="/test/output",
                subject_id="",
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
        )

        result = job_no_subject.get_procedures()

        self.assertIsNone(result)
        mock_file_exists.assert_not_called()

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "_get_file_from_user_defined_directory")
    def test_get_procedures_existing_file(self, mock_get_file, mock_file_exists):
        """Test get_procedures when file already exists"""
        mock_file_exists.return_value = True
        mock_get_file.return_value = {"procedures": "data"}

        result = self.job.get_procedures()

        self.assertEqual(result, {"procedures": "data"})
        mock_file_exists.assert_called_once_with(file_name="procedures.json")
        mock_get_file.assert_called_once_with(file_name="procedures.json")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_procedures_api_success(self, mock_get, mock_file_exists):
        """Test get_procedures when downloading from API successfully"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"procedures": "data"}
        mock_get.return_value = mock_response

        result = self.job.get_procedures()

        self.assertEqual(result, {"procedures": "data"})
        mock_get.assert_called_once_with("http://test-service.com/api/v2/procedures/123456")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_procedures_api_404(self, mock_get, mock_file_exists):
        """Test get_procedures when procedures not found in API"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "Procedures not found"}
        mock_get.return_value = mock_response

        result = self.job.get_procedures()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_procedures_api_other_status(self, mock_get, mock_file_exists):
        """Test get_procedures when API returns other status code"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.json.return_value = {"error": "Server error"}
        mock_get.return_value = mock_response

        result = self.job.get_procedures()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    @patch("logging.error")
    def test_get_procedures_api_exception(self, mock_log_error, mock_get, mock_file_exists):
        """Test get_procedures when API request raises exception"""
        mock_file_exists.return_value = False
        mock_get.side_effect = Exception("Network error")

        result = self.job.get_procedures()

        self.assertIsNone(result)
        mock_log_error.assert_called_once()

    # Tests for other metadata getter methods
    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    def test_get_acquisition_no_file(self, mock_file_exists):
        """Test get_acquisition when no file exists"""
        mock_file_exists.return_value = False

        result = self.job.get_acquisition()

        self.assertIsNone(result)

    @patch("os.makedirs")
    def test_run_mappers_for_acquisition_no_metadata_dir(self, mock_makedirs):
        """Test _run_mappers_for_acquisition when metadata_dir is None"""
        job_settings = JobSettings(
            metadata_dir=None,
            output_dir="/test/output",
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        job = GatherMetadataJob(settings=job_settings)

        result = job._run_mappers_for_acquisition()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    def test_get_instrument_no_file(self, mock_file_exists):
        """Test get_instrument when no file exists"""
        mock_file_exists.return_value = False

        result = self.job.get_instrument()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    def test_get_processing_no_file(self, mock_file_exists):
        """Test get_processing when no file exists"""
        mock_file_exists.return_value = False

        result = self.job.get_processing()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    def test_get_quality_control_no_file(self, mock_file_exists):
        """Test get_quality_control when no file exists"""
        mock_file_exists.return_value = False

        result = self.job.get_quality_control()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    def test_get_model_no_file(self, mock_file_exists):
        """Test get_model when no file exists"""
        mock_file_exists.return_value = False

        result = self.job.get_model()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "_get_file_from_user_defined_directory")
    def test_get_model_existing_file(self, mock_get_file, mock_file_exists):
        """Test get_model when file exists"""
        mock_file_exists.return_value = True
        mock_get_file.return_value = {"model": "data"}

        result = self.job.get_model()

        self.assertEqual(result, {"model": "data"})
        mock_file_exists.assert_called_once_with(file_name="model.json")
        mock_get_file.assert_called_once_with(file_name="model.json")

    # Tests for validate_and_create_metadata method
    def test_validate_and_create_metadata_success_with_raise_if_invalid_false(self):
        """Test validate_and_create_metadata when validation succeeds with raise_if_invalid=False (default)"""
        core_metadata = {
            "data_description": {
                "name": "test_dataset",
                "creation_time": "2023-01-01T12:00:00",
                "institution": {"name": "Allen Institute for Neural Dynamics"},
                "data_level": "processed",  # Use processed to avoid subject_id requirement
                "modalities": ["Behavior videos"],
                "project_name": "Test Project",
                "funding_source": [{"funder": "Test Funder"}],
                "investigators": [{"name": "Test Investigator"}],
            }
        }

        # Mock Metadata to avoid actual validation
        with patch("aind_metadata_mapper.gather_metadata.Metadata") as mock_metadata:
            mock_instance = MagicMock()
            mock_metadata.return_value = mock_instance
            result = self.job.validate_and_create_metadata(core_metadata)
            self.assertEqual(result, mock_instance)

    def test_validate_and_create_metadata_success_with_raise_if_invalid_true(self):
        """Test validate_and_create_metadata when validation succeeds with raise_if_invalid=True"""
        with patch("os.makedirs"):
            strict_settings = JobSettings(
                metadata_dir="/test/metadata",
                output_dir="/test/output",
                subject_id="123456",
                project_name="Test Project",
                modalities=[Modality.ECEPHYS, Modality.BEHAVIOR],
                metadata_service_url="http://test-service.com",
                raise_if_invalid=True,
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
            strict_job = GatherMetadataJob(settings=strict_settings)

        core_metadata = {
            "data_description": {
                "name": "test_dataset",
                "creation_time": "2023-01-01T12:00:00",
                "institution": {"name": "Allen Institute for Neural Dynamics"},
                "data_level": "processed",  # Use processed to avoid subject_id requirement
                "modalities": ["Behavior videos"],
                "project_name": "Test Project",
                "funding_source": [{"funder": "Test Funder"}],
                "investigators": [{"name": "Test Investigator"}],
            }
        }

        # Mock Metadata to avoid actual validation
        with patch("aind_metadata_mapper.gather_metadata.Metadata") as mock_metadata:
            mock_instance = MagicMock()
            mock_metadata.return_value = mock_instance
            result = strict_job.validate_and_create_metadata(core_metadata)
            self.assertEqual(result, mock_instance)

    # Tests for run_job method
    @patch.object(GatherMetadataJob, "build_data_description")
    @patch.object(GatherMetadataJob, "get_subject")
    @patch.object(GatherMetadataJob, "get_procedures")
    @patch.object(GatherMetadataJob, "get_acquisition")
    @patch.object(GatherMetadataJob, "get_instrument")
    @patch.object(GatherMetadataJob, "get_processing")
    @patch.object(GatherMetadataJob, "get_quality_control")
    @patch.object(GatherMetadataJob, "get_model")
    @patch.object(GatherMetadataJob, "validate_and_create_metadata")
    @patch.object(GatherMetadataJob, "_write_json_file")
    def test_run_job_basic(
        self,
        mock_write,
        mock_validate,
        mock_model,
        mock_qc,
        mock_processing,
        mock_instrument,
        mock_acquisition,
        mock_procedures,
        mock_subject,
        mock_data_desc,
    ):
        """Test run_job basic functionality"""
        # Mock all getters to return expected data
        mock_data_desc.return_value = {"name": "test", "data_level": "raw"}
        mock_subject.return_value = None
        mock_procedures.return_value = None
        mock_acquisition.return_value = None
        mock_instrument.return_value = None
        mock_processing.return_value = None
        mock_qc.return_value = None
        mock_model.return_value = None

        # Return a Metadata object from validate_and_create_metadata
        mock_metadata_obj = MagicMock(spec=Metadata)
        mock_validate.return_value = mock_metadata_obj

        self.job.run_job()

        # Verify that validate_and_create_metadata was called
        mock_validate.assert_called_once()

        # Verify that _write_json_file was called for data_description
        calls = mock_write.call_args_list
        data_desc_call = [call for call in calls if call[0][0] == "data_description.json"]
        self.assertEqual(len(data_desc_call), 1)

    @patch.object(GatherMetadataJob, "build_data_description")
    @patch.object(GatherMetadataJob, "get_subject")
    @patch.object(GatherMetadataJob, "get_procedures")
    @patch.object(GatherMetadataJob, "get_acquisition")
    @patch.object(GatherMetadataJob, "get_instrument")
    @patch.object(GatherMetadataJob, "get_processing")
    @patch.object(GatherMetadataJob, "get_quality_control")
    @patch.object(GatherMetadataJob, "get_model")
    @patch.object(GatherMetadataJob, "validate_and_create_metadata")
    @patch.object(GatherMetadataJob, "_write_json_file")
    def test_run_job_with_model_file(
        self,
        mock_write,
        mock_validate,
        mock_model,
        mock_qc,
        mock_processing,
        mock_instrument,
        mock_acquisition,
        mock_procedures,
        mock_subject,
        mock_data_desc,
    ):
        """Test run_job when model file exists"""
        # Mock data description
        mock_data_desc.return_value = {"name": "test", "data_level": "raw"}
        # Mock other getters to return None except model
        mock_subject.return_value = None
        mock_procedures.return_value = None
        mock_acquisition.return_value = None
        mock_instrument.return_value = None
        mock_processing.return_value = None
        mock_qc.return_value = None
        mock_model.return_value = {"model_name": "test_model"}

        # Return a Metadata object from validate_and_create_metadata
        mock_metadata_obj = MagicMock(spec=Metadata)
        mock_validate.return_value = mock_metadata_obj

        self.job.run_job()

        # Verify that model was written
        model_calls = [call for call in mock_write.call_args_list if call[0][0] == "model.json"]
        self.assertEqual(len(model_calls), 1)
        self.assertEqual(model_calls[0][0][1], {"model_name": "test_model"})

    def test_merge_models_instruments(self):
        """Test _merge_models with instrument objects"""
        import json

        from aind_data_schema.core.instrument import Instrument

        with open(TEST_DIR / "resources" / "v2_metadata" / "instrument.json") as f:
            base_instrument = json.load(f)

        instrument1 = base_instrument.copy()
        instrument2 = base_instrument.copy()

        result = self.job._merge_models(Instrument, [instrument1, instrument2])

        self.assertIsInstance(result, dict)
        self.assertIn("instrument_id", result)

    def test_merge_models_acquisition(self):
        """Test _merge_models with acquisition objects"""
        import json

        from aind_data_schema.core.acquisition import Acquisition

        with open(TEST_DIR / "resources" / "v2_metadata" / "acquisition.json") as f:
            base_acquisition = json.load(f)

        acquisition1 = base_acquisition.copy()
        acquisition2 = base_acquisition.copy()

        if "subject_details" in acquisition2:
            del acquisition2["subject_details"]

        result = self.job._merge_models(Acquisition, [acquisition1, acquisition2])

        self.assertIsInstance(result, dict)
        self.assertIn("acquisition_start_time", result)

    def test_merge_models_datetime_serialization(self):
        """Test that merged models can be JSON serialized (datetime objects converted to strings)"""
        # Create two minimal acquisition models with datetime fields
        now = datetime.now(timezone.utc)

        acq1_dict = {
            "subject_id": "test",
            "acquisition_start_time": now.isoformat(),
            "acquisition_end_time": now.isoformat(),
            "instrument_id": "test",
            "acquisition_type": "test",
            "data_streams": [],
        }

        acq2_dict = {
            "subject_id": "test",
            "acquisition_start_time": now.isoformat(),
            "acquisition_end_time": now.isoformat(),
            "instrument_id": "test",
            "acquisition_type": "test",
            "data_streams": [],
        }

        # Merge the two acquisitions using _merge_models
        merged_dict = self.job._merge_models(Acquisition, [acq1_dict, acq2_dict])

        # Verify the merged dict can be JSON serialized
        # This will fail if merged_dict contains datetime objects instead of strings
        json_str = json.dumps(merged_dict, indent=2)
        self.assertIsInstance(json_str, str)
        # Verify datetime fields are strings, not datetime objects
        self.assertIsInstance(merged_dict.get("acquisition_start_time"), str)

    def test_get_instrument_multiple_files(self):
        """Test get_instrument with multiple instrument files"""
        import json
        import shutil
        import tempfile

        temp_dir = tempfile.mkdtemp()
        output_dir = tempfile.mkdtemp()
        test_settings = JobSettings(
            metadata_dir=temp_dir,
            output_dir=output_dir,
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        test_job = GatherMetadataJob(settings=test_settings)

        try:
            with open(TEST_DIR / "resources" / "v2_metadata" / "instrument.json") as f:
                base_instrument = json.load(f)

            instrument1 = base_instrument.copy()
            instrument2 = base_instrument.copy()

            with open(os.path.join(temp_dir, "instrument_123.json"), "w") as f:
                json.dump(instrument1, f)

            with open(os.path.join(temp_dir, "instrument_456.json"), "w") as f:
                json.dump(instrument2, f)

            result = test_job.get_instrument()

            self.assertIsNotNone(result)
            self.assertIsInstance(result, dict)
            self.assertIn("instrument_id", result)

        finally:
            shutil.rmtree(temp_dir)

    def test_get_acquisition_multiple_files(self):
        """Test get_acquisition with multiple acquisition files"""
        import json
        import shutil
        import tempfile

        temp_dir = tempfile.mkdtemp()
        output_dir = tempfile.mkdtemp()
        test_settings = JobSettings(
            metadata_dir=temp_dir,
            output_dir=output_dir,
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        test_job = GatherMetadataJob(settings=test_settings)

        try:
            with open(TEST_DIR / "resources" / "v2_metadata" / "acquisition.json") as f:
                base_acquisition = json.load(f)

            if "subject_details" in base_acquisition:
                del base_acquisition["subject_details"]

            acquisition1 = base_acquisition.copy()
            acquisition2 = base_acquisition.copy()

            # Place acquisition files in output directory since that's where they're read from
            with open(os.path.join(output_dir, "acquisition_789.json"), "w") as f:
                json.dump(acquisition1, f)

            with open(os.path.join(output_dir, "acquisition_012.json"), "w") as f:
                json.dump(acquisition2, f)

            result = test_job.get_acquisition()

            self.assertIsNotNone(result)
            self.assertIsInstance(result, dict)
            self.assertIn("acquisition_start_time", result)

        finally:
            shutil.rmtree(temp_dir)

    def test_get_quality_control_multiple_files(self):
        """Test get_quality_control with multiple quality control files"""
        import json
        import shutil
        import tempfile

        temp_dir = tempfile.mkdtemp()
        output_dir = tempfile.mkdtemp()
        test_settings = JobSettings(
            metadata_dir=temp_dir,
            output_dir=output_dir,
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        test_job = GatherMetadataJob(settings=test_settings)

        try:
            with open(TEST_DIR / "resources" / "v2_metadata" / "quality_control.json") as f:
                base_qc = json.load(f)

            qc1 = base_qc.copy()
            qc2 = base_qc.copy()

            with open(os.path.join(temp_dir, "quality_control_345.json"), "w") as f:
                json.dump(qc1, f)

            with open(os.path.join(temp_dir, "quality_control_678.json"), "w") as f:
                json.dump(qc2, f)

            result = test_job.get_quality_control()

            self.assertIsNotNone(result)
            self.assertIsInstance(result, dict)
            self.assertIn("metrics", result)

        finally:
            shutil.rmtree(temp_dir)

    def test_get_prefixed_files_no_matches(self):
        """Test _get_prefixed_files_from_user_defined_directory with no matching files"""
        import json
        import shutil
        import tempfile

        temp_dir = tempfile.mkdtemp()
        output_dir = tempfile.mkdtemp()
        test_settings = JobSettings(
            metadata_dir=temp_dir,
            output_dir=output_dir,
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        test_job = GatherMetadataJob(settings=test_settings)

        try:
            with open(os.path.join(temp_dir, "other_file.json"), "w") as f:
                json.dump({"data": "test"}, f)

            result = test_job._get_prefixed_files_from_user_defined_directory("instrument")

            self.assertEqual(result, [])

        finally:
            shutil.rmtree(temp_dir)

    def test_get_prefixed_files_mixed_extensions(self):
        """Test _get_prefixed_files_from_user_defined_directory ignores non-json files"""
        import json
        import shutil
        import tempfile

        temp_dir = tempfile.mkdtemp()
        output_dir = tempfile.mkdtemp()
        test_settings = JobSettings(
            metadata_dir=temp_dir,
            output_dir=output_dir,
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
        )
        test_job = GatherMetadataJob(settings=test_settings)

        try:
            with open(os.path.join(temp_dir, "instrument_123.json"), "w") as f:
                json.dump({"instrument_id": "test"}, f)

            with open(os.path.join(temp_dir, "instrument_456.txt"), "w") as f:
                f.write("not json")

            result = test_job._get_prefixed_files_from_user_defined_directory("instrument")

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["instrument_id"], "test")

        finally:
            shutil.rmtree(temp_dir)

    @patch.object(GatherMetadataJob, "get_acquisition")
    @patch("os.makedirs")
    def test_run_job_missing_acquisition_start_time(self, mock_makedirs, mock_acquisition):
        """Test run_job raises error when acquisition_start_time is not provided and no acquisition file exists"""
        mock_acquisition.return_value = None

        job_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS],
            acquisition_start_time=None,
        )
        job = GatherMetadataJob(settings=job_settings)

        with self.assertRaises(ValueError) as context:
            job.run_job()

        self.assertIn("acquisition_start_time is required", str(context.exception))


class TestAddEthicsIdToAcquisition(unittest.TestCase):
    """Tests for _add_ethics_id_to_acquisition method."""

    @patch("aind_metadata_mapper.gather_metadata.get_ethics_id")
    def test_add_ethics_id_when_missing(self, mock_get_ethics_id):
        """Test that ethics_review_id is added when it's None or empty."""
        mock_get_ethics_id.return_value = "2414"
        acquisition = {"subject_id": "test_subject"}
        job = GatherMetadataJob(
            settings=JobSettings(
                output_dir="/tmp", subject_id="test_subject", project_name="Test", modalities=[Modality.ECEPHYS]
            )
        )

        job._add_ethics_id_to_acquisition(acquisition, "test_subject")

        self.assertEqual(acquisition["ethics_review_id"], ["2414"])
        mock_get_ethics_id.assert_called_once_with("test_subject")

    @patch("aind_metadata_mapper.gather_metadata.get_ethics_id")
    def test_verify_ethics_id_when_matches(self, mock_get_ethics_id):
        """Test that no error is raised when ethics_review_id matches the found ethics ID."""
        mock_get_ethics_id.return_value = "2414"
        acquisition = {"subject_id": "test_subject", "ethics_review_id": ["2414"]}
        job = GatherMetadataJob(
            settings=JobSettings(
                output_dir="/tmp", subject_id="test_subject", project_name="Test", modalities=[Modality.ECEPHYS]
            )
        )

        # Should not raise an error
        job._add_ethics_id_to_acquisition(acquisition, "test_subject")

        self.assertEqual(acquisition["ethics_review_id"], ["2414"])

    @patch("aind_metadata_mapper.gather_metadata.get_ethics_id")
    def test_verify_ethics_id_when_mismatch(self, mock_get_ethics_id):
        """Test that ValueError is raised when ethics_review_id doesn't match."""
        mock_get_ethics_id.return_value = "2414"
        acquisition = {"subject_id": "test_subject", "ethics_review_id": ["2115"]}
        job = GatherMetadataJob(
            settings=JobSettings(
                output_dir="/tmp", subject_id="test_subject", project_name="Test", modalities=[Modality.ECEPHYS]
            )
        )

        with self.assertRaises(ValueError) as context:
            job._add_ethics_id_to_acquisition(acquisition, "test_subject")

        self.assertIn("ethics_review_id mismatch", str(context.exception))
        self.assertIn("2115", str(context.exception))
        self.assertIn("2414", str(context.exception))

    @patch("aind_metadata_mapper.gather_metadata.get_ethics_id")
    def test_no_change_when_ethics_id_not_found(self, mock_get_ethics_id):
        """Test that acquisition is not modified when get_ethics_id returns None."""
        mock_get_ethics_id.return_value = None
        acquisition = {"subject_id": "test_subject", "ethics_review_id": ["2115"]}
        job = GatherMetadataJob(
            settings=JobSettings(
                output_dir="/tmp", subject_id="test_subject", project_name="Test", modalities=[Modality.ECEPHYS]
            )
        )

        job._add_ethics_id_to_acquisition(acquisition, "test_subject")

        # Should remain unchanged
        self.assertEqual(acquisition["ethics_review_id"], ["2115"])


if __name__ == "__main__":
    unittest.main()
