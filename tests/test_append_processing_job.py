"""Tests class in append_processing_job module."""

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
    ProcessStage,
)
from aind_data_schema_models.process_names import ProcessName

from aind_metadata_mapper.append_processing_job import (
    AppendProcessingJob,
    JobSettings,
)


class TestJobSettings(unittest.TestCase):
    """Tests for methods in JobSettings class."""

    def test_check_s3_location_and_data_asset_id(self):
        """Tests errors are raised if s3_location is None and data_asset_id is None."""

        new_processing = Processing(data_processes=[])
        # Neither s3_location nor data_asset_id are set
        with self.assertRaises(ValueError):
            JobSettings(docdb_host="http://example.com", docdb_version="v2", processing=new_processing)
        # Both s3_location nor data_asset_id are set
        with self.assertRaises(ValueError):
            JobSettings(
                docdb_host="http://example.com",
                docdb_version="v2",
                processing=new_processing,
                s3_location="s3://example/asset",
                data_asset_id="123-456",
            )
        job_settings = JobSettings(
            docdb_host="http://example.com",
            docdb_version="v2",
            processing=new_processing,
            s3_location="s3://example/asset",
        )
        self.assertIsNotNone(job_settings.s3_location)


class TestAppendProcessingJob(unittest.TestCase):
    """Tests for methods in AppendProcessingJob class."""

    @classmethod
    def setUpClass(cls):
        """Set up class with reusable data models."""

        new_processing_example = Processing(
            data_processes=[
                DataProcess(
                    name="behavior-videos compression",
                    process_type=ProcessName.COMPRESSION,
                    code=Code(url="www.example.com/code", version="0"),
                    experimenters=["person 1"],
                    start_date_time=datetime(2026, 10, 11, 1, 2, 3),
                    stage=ProcessStage.PROCESSING,
                )
            ]
        )
        job_settings_with_location = JobSettings(
            processing=new_processing_example,
            s3_location="s3://example/asset",
            docdb_host="http://example.com",
            docdb_version="v2",
        )
        job_settings_with_asset_id = JobSettings(
            processing=new_processing_example,
            data_asset_id="abc-def",
            docdb_host="http://example.com",
            docdb_version="v2",
        )
        old_processing_example = Processing(
            data_processes=[
                DataProcess(
                    name="ecephys compression",
                    process_type=ProcessName.COMPRESSION,
                    code=Code(url="www.example.com/code2", version="0"),
                    experimenters=["person 2"],
                    start_date_time=datetime(2026, 10, 10, 1, 2, 3),
                    stage=ProcessStage.PROCESSING,
                )
            ]
        )
        docdb_example_record = {
            "_id": "123456",
            "location": "s3://example/asset",
            "processing": old_processing_example.model_dump(mode="json"),
        }
        cls.job_with_location = AppendProcessingJob(settings=job_settings_with_location)
        cls.job_with_id = AppendProcessingJob(settings=job_settings_with_asset_id)
        cls.docdb_record = docdb_example_record

    @patch("aind_data_access_api.document_db.MetadataDbClient")
    def test_fetch_processing_record_by_location(self, mock_docdb_client: MagicMock):
        """Tests _fetch_processing_record when s3_location is set in job settings."""
        mock_docdb_client.retrieve_docdb_records.return_value = [self.docdb_record]
        response = self.job_with_location._fetch_processing_record(docdb_client=mock_docdb_client)
        self.assertIsNotNone(response)

    @patch("aind_data_access_api.document_db.MetadataDbClient")
    def test_fetch_processing_record_by_id(self, mock_docdb_client: MagicMock):
        """Tests _fetch_processing_record when data_asset_id is set in job settings."""
        mock_docdb_client.retrieve_docdb_records.return_value = [self.docdb_record]
        response = self.job_with_id._fetch_processing_record(docdb_client=mock_docdb_client)
        self.assertIsNotNone(response)

    @patch("aind_data_access_api.document_db.MetadataDbClient")
    def test_fetch_processing_too_many_records_error(self, mock_docdb_client: MagicMock):
        """Tests _fetch_processing_record raises an error when more than one docdb record matches."""
        mock_docdb_client.retrieve_docdb_records.return_value = [self.docdb_record, self.docdb_record]
        with self.assertRaises(AssertionError):
            self.job_with_id._fetch_processing_record(docdb_client=mock_docdb_client)

    @patch("aind_data_access_api.document_db.MetadataDbClient")
    def test_fetch_processing_no_records_error(self, mock_docdb_client: MagicMock):
        """Tests _fetch_processing_record raises an error when no docdb record matches."""
        mock_docdb_client.retrieve_docdb_records.return_value = []
        with self.assertRaises(AssertionError):
            self.job_with_id._fetch_processing_record(docdb_client=mock_docdb_client)

    def test_merge_processing_record(self):
        """Tests _merge_processing_record method."""
        old_docdb_record = self.docdb_record
        updated_processing = self.job_with_location._merge_processing_record(docdb_record=old_docdb_record)
        self.assertEqual(2, len(updated_processing["processing"]["data_processes"]))

    @patch("aind_data_access_api.document_db.MetadataDbClient")
    def test_update_processing_record(self, mock_docdb_client: MagicMock):
        """Tests _update_processing_record method."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_docdb_client.upsert_one_record.return_value = mock_response
        self.job_with_location._update_processing_record(
            docdb_client=mock_docdb_client, updated_processing_record={"_id": "123"}
        )
        mock_docdb_client.upsert_one_docdb_record.assert_called_once()

    @patch("aind_metadata_mapper.append_processing_job.AppendProcessingJob._fetch_processing_record")
    @patch("aind_metadata_mapper.append_processing_job.AppendProcessingJob._merge_processing_record")
    @patch("aind_metadata_mapper.append_processing_job.AppendProcessingJob._update_processing_record")
    def test_run_job(
        self, mock_update_record: MagicMock, mock_merge_record: MagicMock, mock_fetch_processing_record: MagicMock
    ):
        """Tests run_job method."""
        mock_fetch_processing_record.return_value = self.docdb_record
        mock_merge_record.return_value = self.docdb_record
        with self.assertLogs(level="INFO") as captured:
            self.job_with_location.run_job()
        mock_update_record.assert_called_once()
        self.assertEqual(5, len(captured.output))


if __name__ == "__main__":
    unittest.main()
