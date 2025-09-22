"""Tests gather_metadata module"""

import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from requests import Response

from aind_metadata_mapper.gather_metadata import GatherMetadataJob
from aind_metadata_mapper.models import JobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))


class TestGatherMetadataJob(unittest.TestCase):
    """Tests methods in GatherMetadataJob class"""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up class with example JobSettings"""
        mock_fail_response = Response()
        mock_fail_response.status_code = 500
        mock_success_response = Response()
        mock_success_response.status_code = 200
        mock_invalid_response = Response()
        mock_invalid_response.status_code = 400
        body = json.dumps({"example": "only"})
        mock_success_response._content = body.encode("utf-8")
        mock_invalid_response._content = body.encode("utf-8")
        example_job_settings = JobSettings(metadata_dir=str(TEST_DIR))
        cls.success_response = mock_success_response
        cls.fail_response = mock_fail_response
        cls.example_job = GatherMetadataJob(settings=example_job_settings)

    @patch("os.path.isfile")
    def test_does_file_exist_in_user_defined_dir(self, mock_isfile: MagicMock):
        """Tests _does_file_exist_in_user_defined_dir"""
        mock_isfile.return_value = True
        self.assertTrue(
            self.example_job._does_file_exist_in_user_defined_dir("example")
        )

    @patch("os.path.isfile")
    def test_does_file_exist_in_user_defined_dir_false(
        self, mock_isfile: MagicMock
    ):
        """Tests _does_file_exist_in_user_defined_dir when False"""
        mock_isfile.return_value = False
        self.assertFalse(
            self.example_job._does_file_exist_in_user_defined_dir("example")
        )

    @patch("builtins.open", new_callable=mock_open, read_data='{"a": "1"}')
    def test_get_file_from_user_defined_directory(self, mock_file: MagicMock):
        """Tests _get_file_from_user_defined_directory"""
        contents = self.example_job._get_file_from_user_defined_directory(
            file_name="example"
        )
        mock_file.assert_called()
        self.assertEqual({"a": "1"}, contents)

    @patch("requests.get")
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._does_file_exist_in_user_defined_dir"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._get_file_from_user_defined_directory"
    )
    def test_get_subject_from_file(
        self,
        mock_get_file: MagicMock,
        mock_does_file_exist: MagicMock,
        mock_requests_get: MagicMock,
    ):
        """Tests get_subject method when a file exists."""

        mock_does_file_exist.return_value = True
        mock_get_file.return_value = {"example": "only"}
        with self.assertLogs(level="DEBUG") as captured:
            contents = self.example_job.get_subject()
        self.assertEqual({"example": "only"}, contents)
        self.assertEqual(2, len(captured.output))
        mock_requests_get.assert_not_called()

    @patch("requests.get")
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._does_file_exist_in_user_defined_dir"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._get_file_from_user_defined_directory"
    )
    def test_get_subject_from_service(
        self,
        mock_get_file: MagicMock,
        mock_does_file_exist: MagicMock,
        mock_requests_get: MagicMock,
    ):
        """Tests get_subject method when a file does not exist."""
        mock_does_file_exist.return_value = False
        mock_requests_get.return_value = self.success_response
        with self.assertLogs(level="DEBUG") as captured:
            contents = self.example_job.get_subject()
        self.assertEqual({"example": "only"}, contents)
        self.assertEqual(2, len(captured.output))
        mock_get_file.assert_not_called()

    @patch("requests.get")
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._does_file_exist_in_user_defined_dir"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._get_file_from_user_defined_directory"
    )
    def test_get_subject_from_service_invalid(
        self,
        mock_get_file: MagicMock,
        mock_does_file_exist: MagicMock,
        mock_requests_get: MagicMock,
    ):
        """Tests get_subject method when the response from metadata service
        has validation errors."""
        mock_does_file_exist.return_value = False
        mock_requests_get.return_value = self.success_response
        with self.assertLogs(level="DEBUG") as captured:
            contents = self.example_job.get_subject()
        self.assertEqual({"example": "only"}, contents)
        self.assertEqual(2, len(captured.output))
        mock_get_file.assert_not_called()

    @patch("requests.get")
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._does_file_exist_in_user_defined_dir"
    )
    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob"
        "._get_file_from_user_defined_directory"
    )
    def test_get_subject_from_service_error(
        self,
        mock_get_file: MagicMock,
        mock_does_file_exist: MagicMock,
        mock_requests_get: MagicMock,
    ):
        """
        Tests get_subject method when a file does not exist and there is an
        error with the metadata service.
        """
        mock_does_file_exist.return_value = False
        mock_requests_get.return_value = self.fail_response
        with self.assertRaises(Exception) as e:
            with self.assertLogs(level="DEBUG") as captured:
                self.example_job.get_subject()
        self.assertIn("500 Server Error", str(e.exception))
        self.assertEqual(2, len(captured.output))
        mock_get_file.assert_not_called()

    @patch(
        "aind_metadata_mapper.gather_metadata.GatherMetadataJob.get_subject"
    )
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_run_job(
        self,
        mock_json_dump: MagicMock,
        mock_file: MagicMock,
        mock_get_subject: MagicMock,
    ):
        """Tests run_job method."""
        mock_open_file = MagicMock()
        mock_file.return_value.__enter__.return_value = mock_open_file
        mock_get_subject.return_value = {"example": "only"}
        with self.assertLogs(level="DEBUG") as captured:
            self.example_job.run_job()
        mock_file.assert_called()
        mock_json_dump.assert_called_once_with(
            {"example": "only"},
            mock_open_file,
            indent=3,
            ensure_ascii=False,
            sort_keys=True,
        )
        self.assertEqual(3, len(captured.output))


if __name__ == "__main__":
    unittest.main()
