"""Tests parsing of session information from bergamo rig."""

import gzip
import json
import os
import pickle
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aind_metadata_mapper.bergamo.session import BergamoEtl, JobSettings

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "bergamo"
)
EXAMPLE_MD_PATH = RESOURCES_DIR / "parsed_metadata.pkl.gz"
EXAMPLE_IMG_PATH = RESOURCES_DIR / "cropped_neuron50_00001.tif"
EXPECTED_SESSION = RESOURCES_DIR / "expected_session.json"


class TestBergamoEtl(unittest.TestCase):
    """Test methods in BergamoEtl class."""

    @classmethod
    def setUpClass(cls):
        """Load record object and user settings before running tests."""
        with gzip.open(EXAMPLE_MD_PATH, "rb") as f:
            raw_md_contents = pickle.load(f)
        with open(EXPECTED_SESSION, "r") as f:
            expected_session_contents = json.load(f)
        cls.example_metadata = raw_md_contents
        cls.example_job_settings = JobSettings(
            input_source=RESOURCES_DIR,
            experimenter_full_name=["John Apple"],
            subject_id="12345",
            imaging_laser_wavelength=920,  # nm
            fov_imaging_depth=200,  # microns
            fov_targeted_structure="Primary Motor Cortex",
            notes="test upload",
        )
        cls.expected_session = expected_session_contents

    def test_get_tif_file_locations(self):
        """Tests _get_tif_file_locations method"""
        etl = BergamoEtl(job_settings=self.example_job_settings)
        locations = etl.get_tif_file_locations()
        expected_paths = {
            "cropped_neuron50": [RESOURCES_DIR / "cropped_neuron50_00001.tif"]
        }
        self.assertEqual(expected_paths, locations)

    def test_class_constructor(self):
        """Tests that the class can be constructed from a json string"""
        settings1 = self.example_job_settings.model_copy(deep=True)
        json_str = settings1.model_dump_json()
        etl_job1 = BergamoEtl(
            job_settings=json_str,
        )
        self.assertEqual(settings1, etl_job1.job_settings)

    def test_flat_dict_to_nested(self):
        """Test util method to convert dictionaries from flat to nested."""
        original_input = {
            "SI.LINE_FORMAT_VERSION": 1,
            "SI.VERSION_UPDATE": 0,
            "SI.acqState": "loop",
            "SI.acqsPerLoop": "10000",
            "SI.errorMsg": "",
            "SI.extTrigEnable": "1",
            "SI.fieldCurvatureRxs": "[]",
            "SI.fieldCurvatureZs": "[]",
            "SI.hBeams.enablePowerBox": "false",
            "SI.hBeams.errorMsg": "",
            "SI.hBeams.lengthConstants": "[200 Inf]",
            "SI.hBeams.name": "SI Beams",
        }

        expected_output = {
            "SI": {
                "LINE_FORMAT_VERSION": 1,
                "VERSION_UPDATE": 0,
                "acqState": "loop",
                "acqsPerLoop": "10000",
                "errorMsg": "",
                "extTrigEnable": "1",
                "fieldCurvatureRxs": "[]",
                "fieldCurvatureZs": "[]",
                "hBeams": {
                    "enablePowerBox": "false",
                    "errorMsg": "",
                    "lengthConstants": "[200 Inf]",
                    "name": "SI Beams",
                },
            }
        }

        actual_output = BergamoEtl.flat_dict_to_nested(original_input)
        self.assertEqual(expected_output, actual_output)

    @patch(
        "aind_metadata_mapper.bergamo.session.BergamoEtl"
        ".get_tif_file_locations"
    )
    @patch(
        "aind_metadata_mapper.bergamo.session.BergamoEtl"
        ".extract_parsed_metadata_info_from_files"
    )
    def test_run_job(self, mock_extract: MagicMock, mock_get_files: MagicMock):
        """Tests run_job method"""
        mock_extract.return_value = self.example_metadata
        etl = BergamoEtl(job_settings=self.example_job_settings)
        response = etl.run_job()
        self.assertEqual(self.expected_session, json.loads(response.data))
        mock_get_files.assert_called_once()


if __name__ == "__main__":
    unittest.main()
