""" SmartSPIM utility tests """

import copy
import os
import unittest
from datetime import datetime
from pathlib import Path

from aind_data_schema.components.coordinates import AnatomicalDirection

from aind_metadata_mapper.smartspim import utils

from ..resources.smartspim.example_metadata import (
    example_filter_mapping,
    example_metadata_info,
)


class TestSmartspimUtils(unittest.TestCase):
    """Tests methods in the SmartSPIM class"""

    def setUp(self):
        """Setting up temporary folder directory"""
        current_path = Path(os.path.abspath(__file__)).parent
        self.test_local_json_path = current_path.joinpath(
            "../resources/smartspim/local_json.json"
        )

        self.test_asi_file_path_morning = current_path.joinpath(
            "../resources/smartspim/" "example_ASI_logging_morning.txt"
        )
        self.test_asi_file_path_afternoon = current_path.joinpath(
            "../resources/smartspim/" "example_ASI_logging_afternoon.txt"
        )

    def test_read_json_as_dict(self):
        """
        Tests successful reading of a dictionary
        """
        expected_result = {"some_key": "some_value"}
        result = utils.read_json_as_dict(self.test_local_json_path)
        self.assertEqual(expected_result, result)

    def test_read_json_as_dict_fails(self):
        """
        Tests succesful reading of a dictionary
        """
        result = utils.read_json_as_dict("./non_existent_json.json")

        self.assertEqual({}, result)

    def test_anatomical_direction(self):
        """Tests the anatomical direction parsing to data schema"""
        an_dirs = {
            "left_to_right": AnatomicalDirection.LR,
            "right_to_left": AnatomicalDirection.RL,
            "anterior_to_posterior": AnatomicalDirection.AP,
            "posterior_to_anterior": AnatomicalDirection.PA,
            "inferior_to_superior": AnatomicalDirection.IS,
            "superior_to_inferior": AnatomicalDirection.SI,
        }

        for str_an_dir, schema_an_dir in an_dirs.items():
            curr_an_dir = utils.get_anatomical_direction(str_an_dir)
            self.assertEqual(schema_an_dir, curr_an_dir)

    def test_make_acq_tiles_res_none(self):
        """
        Tests making tiles based on the data
        schema and microscope metadata
        """
        modified_example_metadata_info = copy.deepcopy(example_metadata_info)
        del modified_example_metadata_info["session_config"]["z_step_um"]

        with self.assertRaises(KeyError):
            utils.make_acq_tiles(
                metadata_dict=modified_example_metadata_info,
                filter_mapping=example_filter_mapping,
            )

    def test_session_end(self):
        """Tests getting the session end time from microscope acquisition"""
        session_end = utils.get_session_end(self.test_asi_file_path_morning)
        expected_datetime = datetime.strptime(
            "2023-10-19 12:00:55", "%Y-%m-%d %H:%M:%S"
        )

        self.assertEqual(expected_datetime, session_end)

        session_end = utils.get_session_end(self.test_asi_file_path_afternoon)
        expected_datetime = datetime.strptime(
            "2023-10-19 0:00:55", "%Y-%m-%d %H:%M:%S"
        )

        self.assertEqual(expected_datetime, session_end)

    def test_get_excitation_emission_waves(self):
        """Test getting the excitation and emmision waves"""
        channels = ["Ex_488_Em_525", "Ex_561_Em_600", "Ex_639_Em_680"]
        expected_excitation_emission_channels = {488: 525, 561: 600, 639: 680}
        excitation_emission_channels = utils.get_excitation_emission_waves(
            channels
        )
        self.assertEqual(
            expected_excitation_emission_channels, excitation_emission_channels
        )


if __name__ == "__main__":
    unittest.main()
