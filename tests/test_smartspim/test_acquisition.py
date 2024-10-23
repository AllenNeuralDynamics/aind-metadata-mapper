"""
Tests the SmartSPIM acquisition metadata creation
"""

import copy
import unittest
from unittest.mock import MagicMock, patch

from aind_data_schema.core import acquisition

from aind_metadata_mapper.smartspim.acquisition import (
    JobSettings,
    SmartspimETL,
)
from tests.test_smartspim.example_metadata import (
    example_filter_mapping,
    example_metadata_info,
    example_processing_manifest,
    example_session_end_time,
)


class TestSmartspimETL(unittest.TestCase):
    """Tests methods in the SmartSPIM class"""

    def setUp(self):
        """Setting up temporary folder directory"""
        self.example_job_settings_success = JobSettings(
            subject_id="000000",
            input_source="SmartSPIM_000000_2024-10-10_10-10-10",
            output_directory="output_folder",
            asi_filename="derivatives/ASI_logging.txt",
            mdata_filename_json="derivatives/metadata.json",
            processing_manifest_path="derivatives/processing_manifest.json",
        )
        self.example_smartspim_etl_success = SmartspimETL(
            job_settings=self.example_job_settings_success
        )

        self.example_job_settings_fail_mouseid = JobSettings(
            subject_id="00000",
            input_source="SmartSPIM_00000_2024-10-10_10-10-10",
            output_directory="output_folder",
            asi_filename="derivatives/ASI_logging.txt",
            mdata_filename_json="derivatives/metadata.json",
            processing_manifest_path="derivatives/processing_manifest.json",
        )
        self.example_smartspim_etl_fail_mouseid = SmartspimETL(
            job_settings=self.example_job_settings_fail_mouseid
        )

    def test_class_constructor(self):
        """Tests that the class can be constructed from a json string"""
        settings1 = self.example_job_settings_success.model_copy(deep=True)
        json_str = settings1.model_dump_json()
        etl_job1 = SmartspimETL(
            job_settings=json_str,
        )
        self.assertEqual(settings1, etl_job1.job_settings)

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    def test_extract(self, mock_extract: MagicMock):
        """Tests the extract private method inside the ETL"""
        mock_extract.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest,
        }

        result = self.example_smartspim_etl_success._extract()

        expected_result = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest,
        }

        self.assertEqual(expected_result, result)

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    def test_transform(self, mock_extract: MagicMock):
        """Tests the transformation that cretes the acquisition.json"""
        mock_extract.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest,
        }

        test_extracted = self.example_smartspim_etl_success._extract()

        result = self.example_smartspim_etl_success._transform(
            metadata_dict=test_extracted
        )
        self.assertEqual(acquisition.Acquisition, type(result))

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    def test_transform_fail_mouseid(self, mock_extract: MagicMock):
        """Tests when the mouse id is not a valid one"""
        mock_extract.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest,
        }

        test_extracted = self.example_smartspim_etl_fail_mouseid._extract()

        with self.assertRaises(ValueError):
            self.example_smartspim_etl_fail_mouseid._transform(
                metadata_dict=test_extracted
            )

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    def test_transform_fail_axes(self, mock_extract_fail_axes: MagicMock):
        """Tests when the axes are not provided"""
        example_processing_manifest_axes_none = copy.deepcopy(
            example_processing_manifest
        )
        example_processing_manifest_axes_none["prelim_acquisition"][
            "axes"
        ] = None
        mock_extract_fail_axes.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest_axes_none,
        }

        test_extracted_fail_axes = (
            self.example_smartspim_etl_success._extract()
        )

        with self.assertRaises(ValueError):
            self.example_smartspim_etl_success._transform(
                metadata_dict=test_extracted_fail_axes
            )

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    def test_transform_fail_immersion(
        self, mock_extracted_fail_immersion: MagicMock
    ):
        """Tests when the immersion is not provided"""
        example_processing_manifest_immersion = copy.deepcopy(
            example_processing_manifest
        )
        example_processing_manifest_immersion["prelim_acquisition"][
            "chamber_immersion"
        ] = None
        mock_extracted_fail_immersion.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest_immersion,
        }

        test_extracted_fail_immersion = (
            self.example_smartspim_etl_success._extract()
        )

        with self.assertRaises(ValueError):
            self.example_smartspim_etl_success._transform(
                metadata_dict=test_extracted_fail_immersion
            )

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    def test_transform_other_chamber_medium(
        self, mock_extracted_other_immersion: MagicMock
    ):
        """Tests when the immersion is not provided"""
        example_processing_manifest_immersion = copy.deepcopy(
            example_processing_manifest
        )
        example_processing_manifest_immersion["prelim_acquisition"][
            "chamber_immersion"
        ]["medium"] = "unknown"

        mock_extracted_other_immersion.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest_immersion,
        }

        test_extracted_other_immersion = (
            self.example_smartspim_etl_success._extract()
        )

        result = self.example_smartspim_etl_success._transform(
            metadata_dict=test_extracted_other_immersion
        )
        self.assertEqual(acquisition.Acquisition, type(result))

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    def test_transform_other_sample_medium(
        self, mock_extracted_other_immersion: MagicMock
    ):
        """Tests when the sample immersion is not provided"""
        example_processing_manifest_immersion = copy.deepcopy(
            example_processing_manifest
        )
        example_processing_manifest_immersion["prelim_acquisition"][
            "sample_immersion"
        ]["medium"] = "Cargille"

        mock_extracted_other_immersion.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest_immersion,
        }

        test_extracted_other_immersion = (
            self.example_smartspim_etl_success._extract()
        )

        result = self.example_smartspim_etl_success._transform(
            metadata_dict=test_extracted_other_immersion
        )
        self.assertEqual(acquisition.Acquisition, type(result))

    @patch("aind_metadata_mapper.smartspim.acquisition.SmartspimETL._extract")
    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    def test_run_job(
        self, mock_file_write: MagicMock, mock_extract: MagicMock
    ):
        """Tests the run job method that creates the acquisition"""
        mock_extract.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "processing_manifest": example_processing_manifest,
        }

        response = self.example_smartspim_etl_success.run_job()
        mock_file_write.assert_called_once()

        self.assertEqual(200, response.status_code)


if __name__ == "__main__":
    unittest.main()
