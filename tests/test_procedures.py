"""Tests parsing of Procedures information from aind-metadata-service."""

import json
import os
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_metadata_service.client import StatusCodes
from requests import Response

from aind_metadata_mapper.procedures import ProceduresEtl

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
EXAMPLES_DIR = RESOURCES_DIR / "procedures_examples"


class ProceduresEtlTest(unittest.TestCase):
    """Tests methods in ProceduresEtl class."""

    @classmethod
    def setUpClass(cls):
        """Create mocked responses for tests to use. Also creates a basic
        job intance."""

        with open(EXAMPLES_DIR / "basic_response.json") as f:
            contents = json.load(f)
        successful_response = Response()
        successful_response.status_code = StatusCodes.VALID_DATA
        successful_response._content = json.dumps(contents).encode("utf-8")
        multiple_subjects_response_contents = {
            "message": "Multiple Items Found.",
            "data": (
                [
                    contents["data"],
                    contents["data"],
                ]
            ),
        }
        multi_response = Response()
        multi_response.status_code = StatusCodes.MULTIPLE_RESPONSES
        multi_response._content = json.dumps(
            multiple_subjects_response_contents
        ).encode("utf-8")
        invalid_response_contents = deepcopy(contents)
        invalid_response_contents["message"] = "Invalid Model."
        invalid_response_contents["data"]["subject_id"] = None
        invalid_response_contents["data"]["subject_procedures"] = []
        invalid_response = Response()
        invalid_response.status_code = StatusCodes.INVALID_DATA
        invalid_response._content = json.dumps(
            invalid_response_contents
        ).encode("utf-8")
        err_response = Response()
        err_response.status_code = StatusCodes.INTERNAL_SERVER_ERROR
        err_message = {"message": "Internal Server Error.", "data": None}
        err_response._content = json.dumps(err_message).encode("utf-8")

        no_data_response = Response()
        no_data_response.status_code = StatusCodes.NO_DATA_FOUND
        no_data_message = {"message": "No Data Found.", "data": None}
        no_data_response._content = json.dumps(no_data_message).encode("utf-8")

        conn_err_response = Response()
        conn_err_response.status_code = StatusCodes.CONNECTION_ERROR
        cls.successful_response = successful_response
        cls.multi_response = multi_response
        cls.invalid_response = invalid_response
        cls.err_response = err_response
        cls.conn_err_response = conn_err_response
        cls.no_data_response = no_data_response
        cls.procedures_etl = ProceduresEtl.from_args(
            [
                "--output-directory",
                "tests",
                "--input-source",
                "some-url",
                "--subject-id",
                "12345",
            ]
        )

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_successful_response(
        self,
        mock_write: MagicMock,
        mock_api_get: MagicMock,
    ):
        """Tests that a successful response is parsed correctly."""

        mock_api_get.return_value = self.successful_response
        self.procedures_etl.run_job()
        mock_write.assert_called_once_with(output_directory=Path("tests"))

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch("logging.warning")
    def test_multi_response(
        self,
        mock_log_warn: MagicMock,
        mock_write: MagicMock,
        mock_api_get: MagicMock,
    ):
        """Tests parsing of multiple responses returned from the server."""

        mock_api_get.return_value = self.multi_response
        self.procedures_etl.run_job()
        mock_log_warn.assert_called_once_with(
            "Procedures: Multiple Items Found."
        )
        mock_write.assert_called_once_with(output_directory=Path("tests"))

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch("logging.warning")
    def test_invalid_response(
        self,
        mock_log_warn: MagicMock,
        mock_write: MagicMock,
        mock_api_get: MagicMock,
    ):
        """Tests a response that is returned, but that the server flagged as
        not being valid."""

        mock_api_get.return_value = self.invalid_response
        self.procedures_etl.run_job()
        self.assertEqual(
            call("Procedures: Invalid Model."), mock_log_warn.mock_calls[0]
        )
        self.assertTrue(
            "Validation errors were found" in str(mock_log_warn.mock_calls[1])
        )
        mock_write.assert_called_once_with(output_directory=Path("tests"))

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_server_error_response(
        self,
        mock_write: MagicMock,
        mock_api_get: MagicMock,
    ):
        """Tests handling of an server error response."""

        mock_api_get.return_value = self.err_response
        with self.assertRaises(Exception) as ctx:
            self.procedures_etl.run_job()
        self.assertEqual(
            "The server reported back an internal error.", str(ctx.exception)
        )
        mock_write.assert_not_called()

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_connection_error_response(
        self,
        mock_write: MagicMock,
        mock_api_get: MagicMock,
    ):
        """Tests a response when unable to connect to the server."""

        mock_api_get.return_value = self.conn_err_response
        with self.assertRaises(Exception) as ctx:
            self.procedures_etl.run_job()
        self.assertEqual(
            "An error occurred connecting to the server.", str(ctx.exception)
        )
        mock_write.assert_not_called()

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_no_data_response(
        self,
        mock_write: MagicMock,
        mock_api_get: MagicMock,
    ):
        """Tests the response where the server couldn't find the subject id
        in the databases."""

        mock_api_get.return_value = self.no_data_response
        with self.assertRaises(Exception) as ctx:
            self.procedures_etl.run_job()
        self.assertEqual(
            "No data found for subject id: 12345.", str(ctx.exception)
        )
        mock_write.assert_not_called()

    @patch(
        "aind_metadata_service.client.AindMetadataServiceClient.get_procedures"
    )
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch("aind_metadata_mapper.core.validate_model")
    def test_mocked_validate_model(
        self,
        mock_validate: MagicMock,
        mock_write: MagicMock,
        mock_api_get: MagicMock,
    ):
        """Tests case where a valid model was found and matches current
        aind-data-schema version."""
        # Since the aind-data-schema is being updated, this is a bit of a hack
        # to return a True to validate_model.
        mocked_response = self.invalid_response
        mocked_response.status_code = StatusCodes.VALID_DATA
        mock_api_get.return_value = mocked_response
        mock_validate.return_value = (None, None)
        self.procedures_etl.run_job()
        mock_write.assert_called_once_with(output_directory=Path("tests"))


if __name__ == "__main__":
    unittest.main()
