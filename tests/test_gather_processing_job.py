"""Tests class in gather_processing_job module."""

import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

from aind_data_schema.base import GenericModel
from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.processing import (
    DataProcess,
    Processing,
    ProcessStage,
)
from aind_data_schema_models.process_names import ProcessName

from aind_metadata_mapper.gather_processing_job import (
    GatherProcessingJob,
    JobSettings,
)


class TestGatherProcessingJob(unittest.TestCase):
    """Tests methods in GatherProcessingJob class."""

    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data='{"data_processes": []}')
    @patch("json.load")
    def test_load_existing_processing_exists_and_valid(
        self, mock_json_load: MagicMock, mock_file_open: MagicMock, mock_exists: MagicMock
    ):
        """Tests load_existing_processing when file exists and is valid."""
        mock_exists.return_value = True
        existing_processing_dict = {
            "data_processes": [
                {
                    "start_date_time": "2024-09-09T01:02:03",
                    "end_date_time": "2024-09-10T01:02:03",
                    "process_type": "Image atlas alignment",
                    "experimenters": ["Existing Experimenter"],
                    "stage": "Processing",
                    "code": {"url": "www.example.com/existing"},
                }
            ]
        }
        mock_json_load.return_value = existing_processing_dict

        example_processing = Processing(data_processes=[])
        example_settings = JobSettings(output_directory="example", processing=example_processing)
        job = GatherProcessingJob(settings=example_settings)

        result = job.load_existing_processing()

        self.assertIsNotNone(result)
        self.assertIsInstance(result, Processing)
        self.assertEqual(len(result.data_processes), 1)
        mock_exists.assert_called_once()
        mock_json_load.assert_called_once()

    @patch("pathlib.Path.exists")
    def test_load_existing_processing_does_not_exist(self, mock_exists: MagicMock):
        """Tests load_existing_processing when file does not exist."""
        mock_exists.return_value = False

        example_processing = Processing(data_processes=[])
        example_settings = JobSettings(output_directory="example", processing=example_processing)
        job = GatherProcessingJob(settings=example_settings)

        result = job.load_existing_processing()

        self.assertIsNone(result)
        mock_exists.assert_called_once()

    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("logging.error")
    def test_load_existing_processing_invalid_schema(
        self, mock_log_error: MagicMock, mock_json_load: MagicMock, mock_file_open: MagicMock, mock_exists: MagicMock
    ):
        """Tests load_existing_processing when file exists but doesn't match Processing schema."""
        mock_exists.return_value = True
        mock_json_load.return_value = {"invalid_field": "value"}

        example_processing = Processing(data_processes=[])
        example_settings = JobSettings(output_directory="example", processing=example_processing)
        job = GatherProcessingJob(settings=example_settings)

        with self.assertRaises(Exception):
            job.load_existing_processing()
        
        mock_log_error.assert_called_once()

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_run_job(self, mock_json_dump: MagicMock, mock_file_open: MagicMock):
        """Tests run_job method with example processing data."""
        # noinspection PyArgumentList
        example_processing = Processing(
            data_processes=[
                DataProcess(
                    start_date_time=datetime(2024, 10, 10, 1, 2, 3),
                    end_date_time=datetime(2024, 10, 11, 1, 2, 3),
                    process_type=ProcessName.COMPRESSION,
                    experimenters=["AIND Scientific Computing"],
                    stage=ProcessStage.PROCESSING,
                    code=Code(
                        url="www.example.com/ephys_compression",
                        parameters=GenericModel(),
                    ),
                ),
                DataProcess(
                    start_date_time=datetime(2024, 10, 10, 1, 2, 3),
                    end_date_time=datetime(2024, 10, 11, 1, 2, 4),
                    process_type=ProcessName.OTHER,
                    experimenters=["AIND Scientific Computing"],
                    stage=ProcessStage.PROCESSING,
                    code=Code(url=""),
                    notes="Data was copied.",
                ),
            ]
        )
        example_settings = JobSettings(output_directory="example", processing=example_processing)
        job = GatherProcessingJob(settings=example_settings)
        job.run_job()
        mock_file_open.assert_has_calls(calls=[call(Path("example") / "processing.json", "w")])
        mock_json_dump.assert_called_once()

    @patch("aind_metadata_mapper.gather_processing_job.GatherProcessingJob.load_existing_processing")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    @patch("logging.error")
    def test_run_job_merge_fails_raises_error(
        self, mock_log_error: MagicMock, mock_json_dump: MagicMock, 
        mock_file_open: MagicMock, mock_load_existing: MagicMock
    ):
        """Tests run_job raises exception when merge fails."""
        existing_processing = Processing(
            data_processes=[
                DataProcess(
                    start_date_time=datetime(2024, 9, 9, 1, 2, 3),
                    end_date_time=datetime(2024, 9, 10, 1, 2, 3),
                    process_type=ProcessName.IMAGE_ATLAS_ALIGNMENT,
                    experimenters=["Existing Experimenter"],
                    stage=ProcessStage.PROCESSING,
                    code=Code(url="www.example.com/existing_process"),
                )
            ]
        )
        mock_load_existing.return_value = existing_processing
        with patch.object(Processing, '__add__', side_effect=Exception("Merge failed")):
            new_processing = Processing(
                data_processes=[
                    DataProcess(
                        start_date_time=datetime(2024, 10, 10, 1, 2, 3),
                        end_date_time=datetime(2024, 10, 11, 1, 2, 3),
                        process_type=ProcessName.COMPRESSION,
                        experimenters=["AIND Scientific Computing"],
                        stage=ProcessStage.PROCESSING,
                        code=Code(url="www.example.com/ephys_compression"),
                    )
                ]
            )
            example_settings = JobSettings(output_directory="example", processing=new_processing)
            job = GatherProcessingJob(settings=example_settings)


            with self.assertRaises(Exception) as context:
                job.run_job()
            
            self.assertIn("Merge failed", str(context.exception))
            mock_log_error.assert_called_once()
            self.assertIn("Failed to merge existing processing.json", str(mock_log_error.call_args))
            mock_json_dump.assert_not_called()


if __name__ == "__main__":
    unittest.main()
