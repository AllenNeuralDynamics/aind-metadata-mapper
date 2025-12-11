"""Tests gather_metadata module

Tests in this file ONLY test helper functions.

Do not test run_job() inside this file, use the integration tests.
"""

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import DataDescriptionSettings, JobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))


class TestGatherMetadataJob(unittest.TestCase):
    """Tests methods in GatherMetadataJob class"""

    @patch("os.makedirs")
    def setUp(self, mock_makedirs):
        """Set up test fixtures"""
        self.test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS, Modality.BEHAVIOR],
            ),
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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
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

    # Tests for _validate_acquisition_start_time method
    @patch("os.makedirs")
    def test_validate_acquisition_start_time_match(self, mock_makedirs):
        """Test _validate_acquisition_start_time when times match"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        job = GatherMetadataJob(settings=test_settings)

        # Test with matching time
        result = job._validate_acquisition_start_time("2023-01-01T12:00:00+00:00")
        self.assertEqual(result, "2023-01-01T12:00:00+00:00")

    @patch("os.makedirs")
    def test_validate_acquisition_start_time_mismatch_raises(self, mock_makedirs):
        """Test _validate_acquisition_start_time raises when times don't match and raise_if_invalid is True"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            raise_if_invalid=True,
        )
        job = GatherMetadataJob(settings=test_settings)

        # Test with mismatched time
        with self.assertRaises(ValueError) as context:
            job._validate_acquisition_start_time("2023-01-01T14:00:00+00:00")

        self.assertIn("acquisition_start_time from acquisition metadata does not match", str(context.exception))

    @patch("logging.error")
    @patch("os.makedirs")
    def test_validate_acquisition_start_time_mismatch_logs(self, mock_makedirs, mock_log_error):
        """Test _validate_acquisition_start_time logs error when times don't match and raise_if_invalid is False"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            raise_if_invalid=False,
        )
        job = GatherMetadataJob(settings=test_settings)

        # Test with mismatched time
        result = job._validate_acquisition_start_time("2023-01-01T14:00:00+00:00")

        # Should return the time and log an error
        self.assertEqual(result, "2023-01-01T14:00:00+00:00")
        mock_log_error.assert_called_once()
        self.assertIn("acquisition_start_time from acquisition metadata does not match", str(mock_log_error.call_args))

    @patch("os.makedirs")
    def test_validate_acquisition_start_time_with_z_suffix(self, mock_makedirs):
        """Test _validate_acquisition_start_time handles Z suffix correctly"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        job = GatherMetadataJob(settings=test_settings)

        # Test with Z suffix
        result = job._validate_acquisition_start_time("2023-01-01T12:00:00Z")
        self.assertEqual(result, "2023-01-01T12:00:00+00:00")

    # Tests for _validate_and_get_subject_id method
    @patch("os.makedirs")
    def test_validate_and_get_subject_id_from_acquisition(self, mock_makedirs):
        """Test _validate_and_get_subject_id gets subject_id from acquisition"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        job = GatherMetadataJob(settings=test_settings)

        acquisition = {"subject_id": "123456"}
        result = job._validate_and_get_subject_id(acquisition)
        self.assertEqual(result, "123456")

    @patch("os.makedirs")
    def test_validate_and_get_subject_id_from_settings(self, mock_makedirs):
        """Test _validate_and_get_subject_id gets subject_id from settings when not in acquisition"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        job = GatherMetadataJob(settings=test_settings)

        acquisition = {}
        result = job._validate_and_get_subject_id(acquisition)
        self.assertEqual(result, "123456")

    @patch("os.makedirs")
    def test_validate_and_get_subject_id_missing_raises(self, mock_makedirs):
        """Test _validate_and_get_subject_id raises when subject_id is missing"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id=None,
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        job = GatherMetadataJob(settings=test_settings)

        acquisition = {}
        with self.assertRaises(ValueError) as context:
            job._validate_and_get_subject_id(acquisition)

        self.assertIn(
            "Either provide acquisition.json with subject_id, or provide subject_id in the settings.",
            str(context.exception),
        )

    @patch("os.makedirs")
    def test_validate_and_get_subject_id_mismatch_raises(self, mock_makedirs):
        """Test _validate_and_get_subject_id raises when subject_ids don't match and raise_if_invalid is True"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            raise_if_invalid=True,
        )
        job = GatherMetadataJob(settings=test_settings)

        acquisition = {"subject_id": "999999"}
        with self.assertRaises(ValueError) as context:
            job._validate_and_get_subject_id(acquisition)

        self.assertIn("subject_id from acquisition metadata", str(context.exception))
        self.assertIn("does not match", str(context.exception))

    @patch("logging.error")
    @patch("os.makedirs")
    def test_validate_and_get_subject_id_mismatch_logs(self, mock_makedirs, mock_log_error):
        """Test _validate_and_get_subject_id logs error when subject_ids don't match and raise_if_invalid is False"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            raise_if_invalid=False,
        )
        job = GatherMetadataJob(settings=test_settings)

        acquisition = {"subject_id": "999999"}
        result = job._validate_and_get_subject_id(acquisition)

        # Should return the acquisition subject_id and log an error
        self.assertEqual(result, "999999")
        mock_log_error.assert_called_once()
        self.assertIn("subject_id from acquisition metadata", str(mock_log_error.call_args))

    @patch("os.makedirs")
    def test_validate_and_get_subject_id_no_acquisition(self, mock_makedirs):
        """Test _validate_and_get_subject_id handles None acquisition"""
        test_settings = JobSettings(
            metadata_dir="/test/metadata",
            output_dir="/test/output",
            subject_id="123456",
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        job = GatherMetadataJob(settings=test_settings)

        result = job._validate_and_get_subject_id(None)
        self.assertEqual(result, "123456")

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
                data_description_settings=DataDescriptionSettings(
                    project_name="",
                    modalities=[Modality.ECEPHYS],
                ),
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
        )
        funding = job_no_project.get_funding()

        self.assertEqual(funding, [])
        mock_get.assert_not_called()
        mock_makedirs.assert_called()

    @patch("requests.get")
    def test_get_funding_success_single_result(self, mock_get):
        """Test get_funding with successful single result"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "funder": "Test Funder",
                "award_number": "12345",
            }
        ]
        mock_get.return_value = mock_response

        funding = self.job.get_funding()

        expected_funding = [{"funder": "Test Funder", "award_number": "12345"}]
        self.assertEqual(funding, expected_funding)

    @patch("requests.get")
    def test_get_funding_success_multiple_results(self, mock_get):
        """Test get_funding with successful multiple results"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "funder": "Funder 1",
                "award_number": "111",
            },
            {
                "funder": "Funder 2",
                "award_number": "222",
            },
        ]
        mock_get.return_value = mock_response

        funding = self.job.get_funding()

        self.assertEqual(len(funding), 2)

    @patch("requests.get")
    @patch("os.makedirs")
    def test_get_investigators_no_project_name(self, mock_makedirs, mock_get):
        """Test get_investigators when no project name is provided"""
        job_no_project = GatherMetadataJob(
            JobSettings(
                metadata_dir="/test",
                output_dir="/test/output",
                subject_id="test_subject",
                data_description_settings=DataDescriptionSettings(
                    project_name="",
                    modalities=[Modality.ECEPHYS],
                ),
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
        )
        investigators = job_no_project.get_investigators()

        self.assertEqual(investigators, [])
        mock_get.assert_not_called()
        mock_makedirs.assert_called()

    @patch("aind_metadata_mapper.gather_metadata.metadata_service_helper")
    @patch("os.makedirs")
    def test_get_investigators_helper_issue(self, mock_makedirs, mock_metadata_helper):
        """Test get_investigators when helper function returns None"""
        job_no_project = GatherMetadataJob(
            JobSettings(
                metadata_dir="/test",
                output_dir="/test/output",
                subject_id="test_subject",
                data_description_settings=DataDescriptionSettings(
                    project_name="Some Project",
                    modalities=[Modality.ECEPHYS],
                ),
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
        )
        mock_metadata_helper.return_value = None
        investigators = job_no_project.get_investigators()

        self.assertEqual(investigators, [])
        mock_makedirs.assert_called()

    @patch("requests.get")
    def test_get_investigators_success_single_result(self, mock_get):
        """Test get_investigators with successful single result"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "object_type": "Person",
                "name": "Jane Doe",
                "registry": "Open Researcher and Contributor ID (ORCID)",
                "registry_identifier": None,
            }
        ]
        mock_get.return_value = mock_response

        investigators = self.job.get_investigators()

        expected_investigators = [
            {
                "object_type": "Person",
                "name": "Jane Doe",
                "registry": "Open Researcher and Contributor ID (ORCID)",
                "registry_identifier": None,
            }
        ]
        self.assertEqual(investigators, expected_investigators)

    @patch("requests.get")
    def test_get_investigators_success_multiple_results(self, mock_get):
        """Test get_investigators with successful multiple results"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "object_type": "Person",
                "name": "Jane Doe",
                "registry": "Open Researcher and Contributor ID (ORCID)",
                "registry_identifier": None,
            },
            {
                "object_type": "Person",
                "name": "John Smith",
                "registry": "Open Researcher and Contributor ID (ORCID)",
                "registry_identifier": None,
            },
        ]
        mock_get.return_value = mock_response

        investigators = self.job.get_investigators()

        self.assertEqual(len(investigators), 2)

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

        result = self.job.build_data_description(acquisition_start_time="2023-01-01T12:00:00", subject_id="123456")

        self.assertEqual(result, {"existing": "data"})
        mock_file_exists.assert_called_once_with(file_name="data_description.json")
        mock_get_file.assert_called_once_with(file_name="data_description.json")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "get_funding")
    @patch.object(GatherMetadataJob, "get_investigators")
    @patch("aind_metadata_mapper.gather_metadata.datetime")
    def test_build_data_description_new_file(
        self, mock_datetime, mock_get_investigators, mock_get_funding, mock_file_exists
    ):
        """Test build_data_description when creating new file"""
        mock_file_exists.return_value = False
        mock_get_funding.return_value = [
            {
                "object_type": "Funding",
                "funder": Organization.NIMH,
                "grant_number": "12345",
                "fundee": [
                    {
                        "object_type": "Person",
                        "name": "Jane Doe",
                        "registry": "Addgene (ADDGENE)",
                        "registry_identifier": None,
                    }
                ],
            }
        ]
        mock_get_investigators.return_value = [
            {"object_type": "Person", "name": "Jane Doe", "registry": "Addgene (ADDGENE)", "registry_identifier": None}
        ]
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = self.job.build_data_description(acquisition_start_time="2023-01-01T12:00:00", subject_id="123456")

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
                data_description_settings=DataDescriptionSettings(
                    project_name="Test Project",
                    modalities=[Modality.ECEPHYS],
                ),
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
    @patch("aind_metadata_mapper.gather_metadata.get_subject")
    def test_get_subject_api_success(self, mock_get_subject, mock_file_exists):
        """Test get_subject when downloading from API successfully"""
        mock_file_exists.return_value = False
        mock_get_subject.return_value = {"subject_id": "123456"}

        result = self.job.get_subject()

        self.assertEqual(result, {"subject_id": "123456"})
        mock_get_subject.assert_called_once_with("123456", base_url="http://test-service.com/api/v2/subject/")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.get_subject")
    def test_get_subject_api_404(self, mock_get_subject, mock_file_exists):
        """Test get_subject when subject not found in API"""
        mock_file_exists.return_value = False
        mock_get_subject.return_value = None

        result = self.job.get_subject()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.get_subject")
    def test_get_subject_api_other_error(self, mock_get_subject, mock_file_exists):
        """Test get_subject when API returns other error"""
        mock_file_exists.return_value = False
        mock_get_subject.return_value = None

        result = self.job.get_subject()

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.get_subject")
    def test_get_subject_api_exception(self, mock_get_subject, mock_file_exists):
        """Test get_subject when API request raises exception"""
        mock_file_exists.return_value = False
        mock_get_subject.return_value = None

        result = self.job.get_subject()

        self.assertIsNone(result)

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
                data_description_settings=DataDescriptionSettings(
                    project_name="Test Project",
                    modalities=[Modality.ECEPHYS],
                ),
                acquisition_start_time=datetime(2023, 1, 1, 12, 0, 0),
            )
        )

        result = job_no_subject.get_procedures(subject_id="")

        self.assertIsNone(result)
        mock_file_exists.assert_not_called()

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch.object(GatherMetadataJob, "_get_file_from_user_defined_directory")
    def test_get_procedures_existing_file(self, mock_get_file, mock_file_exists):
        """Test get_procedures when file already exists"""
        mock_file_exists.return_value = True
        mock_get_file.return_value = {"procedures": "data"}

        result = self.job.get_procedures(subject_id="123456")

        self.assertEqual(result, {"procedures": "data"})
        mock_file_exists.assert_called_once_with(file_name="procedures.json")
        mock_get_file.assert_called_once_with(file_name="procedures.json")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.get_procedures")
    def test_get_procedures_api_success(self, mock_get_procedures, mock_file_exists):
        """Test get_procedures when downloading from API successfully"""
        mock_file_exists.return_value = False
        mock_get_procedures.return_value = {"procedures": "data"}

        result = self.job.get_procedures(subject_id="123456")

        self.assertEqual(result, {"procedures": "data"})
        mock_get_procedures.assert_called_once_with("123456", base_url="http://test-service.com/api/v2/procedures/")

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.get_procedures")
    def test_get_procedures_api_404(self, mock_get_procedures, mock_file_exists):
        """Test get_procedures when procedures not found in API"""
        mock_file_exists.return_value = False
        mock_get_procedures.return_value = None

        result = self.job.get_procedures(subject_id="123456")

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.get_procedures")
    def test_get_procedures_api_other_status(self, mock_get_procedures, mock_file_exists):
        """Test get_procedures when API returns other status code"""
        mock_file_exists.return_value = False
        mock_get_procedures.return_value = None

        result = self.job.get_procedures(subject_id="123456")

        self.assertIsNone(result)

    @patch.object(GatherMetadataJob, "_does_file_exist_in_user_defined_dir")
    @patch("aind_metadata_mapper.gather_metadata.get_procedures")
    def test_get_procedures_api_exception(self, mock_get_procedures, mock_file_exists):
        """Test get_procedures when API request raises exception"""
        mock_file_exists.return_value = False
        mock_get_procedures.return_value = None

        result = self.job.get_procedures(subject_id="123456")

        self.assertIsNone(result)

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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
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
                data_description_settings=DataDescriptionSettings(
                    project_name="Test Project",
                    modalities=[Modality.ECEPHYS, Modality.BEHAVIOR],
                ),
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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
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

            with open(os.path.join(temp_dir, "acquisition_789.json"), "w") as f:
                json.dump(acquisition1, f)

            with open(os.path.join(temp_dir, "acquisition_012.json"), "w") as f:
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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
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
            data_description_settings=DataDescriptionSettings(
                project_name="Test Project",
                modalities=[Modality.ECEPHYS],
            ),
            acquisition_start_time=None,
        )
        job = GatherMetadataJob(settings=job_settings)

        with self.assertRaises(ValueError) as context:
            job.run_job()

        self.assertIn("acquisition_start_time is required", str(context.exception))


if __name__ == "__main__":
    unittest.main()
