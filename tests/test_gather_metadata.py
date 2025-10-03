"""Tests gather_metadata module"""

import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.core.metadata import Metadata
from aind_data_schema_models.organizations import Organization
from aind_data_schema_models.modalities import Modality
from aind_data_schema.components.identifiers import Person

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))


class TestGatherMetadataJob(unittest.TestCase):
    """Tests methods in GatherMetadataJob class"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_settings = JobSettings(
            metadata_dir="/test/metadata",
            subject_id="123456",
            project_name="Test Project",
            modalities=[Modality.ECEPHYS, Modality.BEHAVIOR],
            metadata_service_url="http://test-service.com",
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
    def test_get_funding_no_project_name(self, mock_get):
        """Test get_funding when no project name is provided"""
        job_no_project = GatherMetadataJob(
            JobSettings(metadata_dir="/test", subject_id="test_subject", project_name="", modalities=[Modality.ECEPHYS])
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
        mock_response.json.return_value = {
            "data": {
                "funder": "Test Funder",
                "award_number": "12345",
                "investigators": [
                    Person(name="John Doe").model_dump(),
                    Person(name="Jane Smith").model_dump(),
                ],
            }
        }
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        expected_funding = [{"funder": "Test Funder", "award_number": "12345"}]
        self.assertEqual(funding, expected_funding)
        self.assertEqual(len(investigators), 2)
        self.assertIsInstance(investigators[0], dict)

    @patch("requests.get")
    def test_get_funding_success_multiple_results(self, mock_get):
        """Test get_funding with successful multiple results (300 status)"""
        mock_response = MagicMock()
        mock_response.status_code = 300
        mock_response.json.return_value = {
            "data": [
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
        }
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        self.assertEqual(len(funding), 2)
        self.assertEqual(len(investigators), 3)  # Alice, Bob, Charlie

    @patch("requests.get")
    def test_get_funding_empty_investigators(self, mock_get):
        """Test get_funding with empty investigators"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "funder": "Test Funder",
                "award_number": "12345",
                "investigators": [],
            }
        }
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        self.assertEqual(len(investigators), 0)

    @patch("requests.get")
    @patch("logging.warning")
    def test_get_funding_http_error(self, mock_warning, mock_get):
        """Test get_funding with HTTP error"""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        funding, investigators = self.job.get_funding()

        self.assertEqual(funding, [])
        self.assertEqual(investigators, [])
        mock_warning.assert_called()

    @patch("requests.get")
    @patch("logging.warning")
    def test_get_funding_request_exception(self, mock_warning, mock_get):
        """Test get_funding with request exception"""
        mock_get.side_effect = Exception("Network error")

        funding, investigators = self.job.get_funding()

        self.assertEqual(funding, [])
        self.assertEqual(investigators, [])
        mock_warning.assert_called()

    # Tests for _write_json_file method
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_write_json_file(self, mock_json_dump, mock_file):
        """Test _write_json_file method"""
        test_data = {"key": "value", "number": 123}

        self.job._write_json_file("output.json", test_data)

        mock_file.assert_called_once_with("/test/metadata/output.json", "w")
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

        result = self.job.build_data_description()

        self.assertEqual(result, {"existing": "data"})
        mock_file_exists.assert_called_once_with(file_name="data_description.json")
        mock_get_file.assert_called_once_with(file_name="data_description.json")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "get_funding")
    @patch("aind_metadata_mapper.gather_metadata.datetime")
    def test_build_data_description_new_file(self, mock_datetime, mock_get_funding, mock_file_exists):
        """Test build_data_description when creating new file"""
        mock_file_exists.return_value = False
        # Return valid funding data structure
        mock_get_funding.return_value = (
            [{"funder": Organization.NIMH, "grant_number": "12345"}],
            [{"name": "Test Investigator"}],
        )
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = self.job.build_data_description()

        self.assertIn("creation_time", result)
        self.assertEqual(result["project_name"], "Test Project")
        self.assertEqual(len(result["modalities"]), 2)

    # Tests for get_subject method
    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    def test_get_subject_no_subject_id(self, mock_file_exists):
        """Test get_subject when no subject_id is provided"""
        job_no_subject = GatherMetadataJob(
            JobSettings(metadata_dir="/test", subject_id="", project_name="Test Project", modalities=[Modality.ECEPHYS])
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
        mock_response.json.return_value = {"data": {"subject_id": "123456"}}
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
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "Subject not found"}
        mock_get.return_value = mock_response

        result = self.job.get_subject()

        self.assertEqual(result, {"error": "Subject not found"})

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_subject_api_other_error(self, mock_get, mock_file_exists):
        """Test get_subject when API returns other error"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "Server error"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.job.get_subject()

        self.assertEqual(result, {"error": "Server error"})

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
    def test_get_procedures_no_subject_id(self, mock_file_exists):
        """Test get_procedures when no subject_id is provided"""
        job_no_subject = GatherMetadataJob(
            JobSettings(metadata_dir="/test", subject_id="", project_name="Test Project", modalities=[Modality.ECEPHYS])
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
        mock_response.json.return_value = {"data": {"procedures": "data"}}
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
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "Procedures not found"}
        mock_get.return_value = mock_response

        result = self.job.get_procedures()

        self.assertEqual(result, {"error": "Procedures not found"})

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("requests.get")
    def test_get_procedures_api_other_status(self, mock_get, mock_file_exists):
        """Test get_procedures when API returns other status code"""
        mock_file_exists.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "Server error"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.job.get_procedures()

        self.assertEqual(result, {"error": "Server error"})

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
    def test_validate_and_create_metadata_success(self):
        """Test validate_and_create_metadata when validation succeeds"""
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

        # Mock create_metadata_json to return a dict (ValidationError path)
        with patch("aind_metadata_mapper.gather_metadata.create_metadata_json") as mock_create:
            mock_create.return_value = {"name": "test_dataset", "location": ""}
            result = self.job.validate_and_create_metadata(core_metadata)
            self.assertEqual(result, {"name": "test_dataset", "location": ""})

    # Tests for run_job method - metadata dict vs object handling
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
    def test_run_job_with_dict_metadata(
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
        """Test run_job when validate_and_create_metadata returns a dict"""
        # Mock all getters to return None to focus on the metadata dict handling
        mock_data_desc.return_value = {"name": "test", "data_level": "raw"}
        mock_subject.return_value = None
        mock_procedures.return_value = None
        mock_acquisition.return_value = None
        mock_instrument.return_value = None
        mock_processing.return_value = None
        mock_qc.return_value = None
        mock_model.return_value = None

        # Return a dict from validate_and_create_metadata
        mock_validate.return_value = {"name": "test_dataset", "location": ""}

        self.job.run_job()

        # Verify that _write_json_file was called for the metadata dict
        calls = mock_write.call_args_list
        metadata_call = [call for call in calls if call[0][0] == "metadata.nd.json"]
        self.assertEqual(len(metadata_call), 1)
        self.assertEqual(metadata_call[0][0][1], {"name": "test_dataset", "location": ""})

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
    def test_run_job_with_metadata_object(
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
        """Test run_job when validate_and_create_metadata returns a Metadata object"""
        # Mock all getters to return None to focus on the metadata object handling
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

        # Verify that write_standard_file was called on the metadata object
        mock_metadata_obj.write_standard_file.assert_called_once_with(
            output_directory=Path("/test/metadata"), suffix="nd.json"
        )

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

        # Return a dict from validate_and_create_metadata
        mock_validate.return_value = {"name": "test_dataset", "location": ""}

        self.job.run_job()

        # Verify that model was written
        model_calls = [call for call in mock_write.call_args_list if call[0][0] == "model.json"]
        self.assertEqual(len(model_calls), 1)
        self.assertEqual(model_calls[0][0][1], {"model_name": "test_model"})


if __name__ == "__main__":
    unittest.main()
