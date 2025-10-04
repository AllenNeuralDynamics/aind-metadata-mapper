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

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_run_job(self, mock_json_dump: MagicMock, mock_file_open: MagicMock):
        """Tests run_job method."""
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
                        parameters=GenericModel(compression_name="BLOSC"),
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


if __name__ == "__main__":
    unittest.main()
