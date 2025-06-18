"""
Tests the SmartSPIM acquisition metadata creation
"""

import copy
import os
import unittest
import json
from datetime import datetime
from unittest.mock import MagicMock, patch
from aind_data_schema.core import acquisition
from pathlib import Path
from aind_metadata_mapper.smartspim.acquisition import (
    JobSettings,
    SmartspimETL,
    SlimsImmersionMedium,
)
from tests.test_smartspim.example_metadata import (
    example_filter_mapping,
    example_metadata_info,
    example_processing_manifest,
    example_session_end_time,
    example_imaging_info_from_slims,
)
from aind_data_schema.components.coordinates import AnatomicalDirection
from aind_data_schema.components.devices import ImmersionMedium
from aind_data_schema.core.acquisition import ProcessingSteps, ProcessName

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "smartspim"
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
            metadata_service_path="http://acme.test",
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
            metadata_service_path="http://acme.test",
        )
        self.example_smartspim_etl_fail_mouseid = SmartspimETL(
            job_settings=self.example_job_settings_fail_mouseid
        )

        with open(RESOURCES_DIR / "expected_acquisition.json", "r") as f:
            self.expected_acquisition = json.load(f)

    def test_class_constructor(self):
        """Tests that the class can be constructed from a json string"""
        settings1 = self.example_job_settings_success.model_copy(deep=True)
        json_str = settings1.model_dump_json()
        etl_job1 = SmartspimETL(
            job_settings=json_str,
        )
        self.assertEqual(settings1, etl_job1.job_settings)

    @patch(
        "aind_metadata_mapper.smartspim.acquisition.SmartspimETL."
        "_extract_metadata_from_microscope_files"
    )
    def test_extract_metadata_from_microscope_files(
        self, mock_extract: MagicMock
    ):
        """Tests the extract method inside the ETL"""
        mock_extract.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
        }

        result = (
            self.example_smartspim_etl_success.
            _extract_metadata_from_microscope_files()
        )

        expected_result = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
        }

        self.assertEqual(expected_result, result)

    @patch("aind_metadata_mapper.smartspim.acquisition.requests.get")
    def test_extract_metadata_from_slims_success(self, mock_get: MagicMock):
        """Tests that metadata is extracted from slims as expected."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [example_imaging_info_from_slims]
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = (
            self.example_smartspim_etl_success._extract_metadata_from_slims(
                start_date_gte=example_session_end_time,
                end_date_lte=example_session_end_time,
            )
        )
        self.assertEqual(example_imaging_info_from_slims, result)
        mock_get.assert_called_once()

    @patch("aind_metadata_mapper.smartspim.acquisition.requests.get")
    def test_extract_metadata_from_slims_no_data_found(self, mock_get):
        """Tests _extract_metadata_from_slims if no data found."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = (
            self.example_smartspim_etl_success._extract_metadata_from_slims()
        )
        self.assertEqual({}, result)

    @patch("aind_metadata_mapper.smartspim.acquisition.requests.get")
    def test_extract_metadata_from_slims_multiple_responses(self, mock_get):
        """Tests _extract_metadata_from_slims if multiple sessions found"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"example_id": 1}, {"example_id": 2}]
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        with self.assertRaises(ValueError):
            self.example_smartspim_etl_success._extract_metadata_from_slims()
        mock_get.assert_called_once()

    def test_map_axes(self):
        """Tests _map_axes correctly maps anatomical directions to axes"""
        x_dir = "Left to Right"
        y_dir = "Anterior to Posterior"
        z_dir = "Superior to Inferior"
        axes_list = SmartspimETL._map_axes(x=x_dir, y=y_dir, z=z_dir)

        expected_dirs = {
            "X": AnatomicalDirection.LR,
            "Y": AnatomicalDirection.AP,
            "Z": AnatomicalDirection.SI,
        }
        self.assertEqual(3, len(axes_list))
        for axis in axes_list:
            self.assertIn(axis.name, expected_dirs)
            self.assertEqual(expected_dirs[axis.name], axis.direction)
            if axis.name == "X":
                self.assertEqual(2, axis.dimension)
            elif axis.name == "Y":
                self.assertEqual(1, axis.dimension)
            elif axis.name == "Z":
                self.assertEqual(0, axis.dimension)

    def test_map_immersion_medium(self):
        """Tests _map_immersion_medium correctly maps to ImmersionMedium"""
        mm = SmartspimETL._map_immersion_medium
        self.assertEqual(SlimsImmersionMedium.DIH2O.value, "diH2O")
        self.assertEqual(
            SlimsImmersionMedium.CARGILLE_OIL_152.value, "Cargille Oil 1.5200"
        )
        self.assertEqual(
            ImmersionMedium.WATER, mm(SlimsImmersionMedium.DIH2O.value)
        )
        self.assertEqual(
            ImmersionMedium.OIL,
            mm(SlimsImmersionMedium.CARGILLE_OIL_152.value),
        )
        self.assertEqual(
            ImmersionMedium.OIL,
            mm(SlimsImmersionMedium.CARGILLE_OIL_153.value),
        )
        self.assertEqual(
            ImmersionMedium.ECI, mm(SlimsImmersionMedium.ETHYL_CINNAMATE.value)
        )
        self.assertEqual(ImmersionMedium.OTHER, mm("unknown_medium"))
        self.assertEqual(ImmersionMedium.OTHER, mm(None))

    def test_map_processing_steps(self):
        """Tests that processing steps are mapped as expected"""
        slims_data = {
            "imaging_channels": [
                "Laser = 561; Emission Filter = 593/40",
                "Laser = 639; Emission Filter = 667/30",
            ],
            "stitching_channels": "Laser = 639, Emission Filter = 667/30",
            "ccf_registration_channels": (
                "Laser = 639, Emission Filter = 667/30"
            ),
        }
        expected_steps = [
            ProcessingSteps(
                channel_name="Ex_561_Em_593",
                process_name=[
                    ProcessName.IMAGE_FLAT_FIELD_CORRECTION,
                    ProcessName.IMAGE_DESTRIPING,
                    ProcessName.IMAGE_TILE_FUSING,
                ],
            ),
            ProcessingSteps(
                channel_name="Ex_639_Em_667",
                process_name=[
                    ProcessName.IMAGE_FLAT_FIELD_CORRECTION,
                    ProcessName.IMAGE_DESTRIPING,
                    ProcessName.IMAGE_TILE_FUSING,
                    ProcessName.IMAGE_ATLAS_ALIGNMENT,
                    ProcessName.IMAGE_TILE_ALIGNMENT,
                ],
            ),
        ]
        steps = self.example_smartspim_etl_success._map_processing_steps(
            slims_data
        )
        actual_channel_names = [s.channel_name for s in steps]
        expected_channel_names = [s.channel_name for s in expected_steps]
        self.assertCountEqual(actual_channel_names, expected_channel_names)

        # For each channel, compare process_name without order
        for expected in expected_steps:
            actual = next(
                s for s in steps if s.channel_name == expected.channel_name
            )
            self.assertCountEqual(
                actual.process_name,
                expected.process_name,
                msg=f"Mismatch in process_name {expected.channel_name}",
            )

    def test_transform(self):
        """Tests that _transform combines microscope metadata
        and SLIMS data into a complete Acquisition model"""
        metadata_dict = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
        }
        slims_data = copy.deepcopy(example_imaging_info_from_slims)
        acq = self.example_smartspim_etl_success._transform(
            metadata_dict=metadata_dict, slims_data=slims_data
        )
        self.assertEqual(type(acq), acquisition.Acquisition)

    @patch(
        "aind_metadata_mapper.smartspim.acquisition.SmartspimETL."
        "_extract_metadata_from_microscope_files"
    )
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

        test_extracted = (
            self.example_smartspim_etl_fail_mouseid.
            _extract_metadata_from_microscope_files()
        )

        with self.assertRaises(ValueError):
            self.example_smartspim_etl_fail_mouseid._transform(
                metadata_dict=test_extracted,
                slims_data=example_imaging_info_from_slims,
            )

    def test_transform_missing_axes(self):
        """Tests that _transform throws error if an axis is missing"""
        # Recreated behavior as it was for processing manifest
        slims_data = copy.deepcopy(example_imaging_info_from_slims)
        slims_data["z_direction"] = None
        metadata_dict = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
        }
        # If an axis direction is None, get_anatomical_direction will fail
        with self.assertRaises(Exception):
            self.example_smartspim_etl_success._transform(
                metadata_dict=metadata_dict, slims_data=slims_data
            )

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch(
        "aind_metadata_mapper.smartspim.acquisition.SmartspimETL."
        "_extract_metadata_from_slims"
    )
    @patch(
        "aind_metadata_mapper.smartspim.acquisition.SmartspimETL."
        "_extract_metadata_from_microscope_files"
    )
    def test_run_job_success(
        self, mock_extract_microscope, mock_extract_slims, mock_write
    ):
        """Tests that run_job runs as expected."""
        mock_extract_microscope.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "session_start_time": datetime.strptime(
                "2024-10-10_10-10-10", "%Y-%m-%d_%H-%M-%S"
            ),
        }
        mock_extract_slims.return_value = example_imaging_info_from_slims
        mock_write.return_value = None

        response = self.example_smartspim_etl_success.run_job()
        mock_write.assert_called_once()
        self.assertEqual(200, response.status_code)

    @patch("aind_data_schema.base.AindCoreModel.write_standard_file")
    @patch(
        "aind_metadata_mapper.smartspim.acquisition.SmartspimETL."
        "_extract_metadata_from_slims"
    )
    @patch(
        "aind_metadata_mapper.smartspim.acquisition.SmartspimETL."
        "_extract_metadata_from_microscope_files"
    )
    def test_run_job_success_slims_datetime(
        self, mock_extract_microscope, mock_extract_slims, mock_write
    ):
        """Tests that run_job runs as expected."""
        mock_extract_microscope.return_value = {
            "session_config": example_metadata_info["session_config"],
            "wavelength_config": example_metadata_info["wavelength_config"],
            "tile_config": example_metadata_info["tile_config"],
            "session_end_time": example_session_end_time,
            "filter_mapping": example_filter_mapping,
            "session_start_time": datetime.strptime(
                "2024-10-10_10-10-10", "%Y-%m-%d_%H-%M-%S"
            ),
        }
        mock_extract_slims.return_value = example_imaging_info_from_slims
        mock_write.return_value = None
        job_settings = self.example_job_settings_success.model_copy(deep=True)
        job_settings.slims_datetime = "2024-10-10_10-10-10"
        etl = SmartspimETL(
            job_settings=job_settings
        )
        response = etl.run_job()
        mock_write.assert_called_once()
        self.assertEqual(200, response.status_code)


if __name__ == "__main__":
    unittest.main()
