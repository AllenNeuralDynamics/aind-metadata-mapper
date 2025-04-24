""" SmartSPIM utility tests """

import copy
import os
import unittest
from datetime import datetime
from pathlib import Path

from aind_data_schema.components.coordinates import AnatomicalDirection

from aind_metadata_mapper.smartspim import utils
from tests.test_smartspim.example_metadata import (
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
            "Right to Left": AnatomicalDirection.RL,
            "anterior_to_posterior": AnatomicalDirection.AP,
            "posterior_to_anterior": AnatomicalDirection.PA,
            "inferior to superior": AnatomicalDirection.IS,
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

    def test_parse_channel_name(self):
        """Test parsing raw channel strings to standard format"""
        raw1 = "Laser = 445; Emission Filter = 469/35"
        self.assertEqual("Ex_445_Em_469", utils.parse_channel_name(raw1))

        raw2 = "Laser = 639, Emission Filter = 667/30"
        self.assertEqual("Ex_639_Em_667", utils.parse_channel_name(raw2))

    def test_ensure_list(self):
        """Test converting different inputs to lists"""
        self.assertEqual([1, 2, 3], utils.ensure_list([1, 2, 3]))
        self.assertEqual(["hello"], utils.ensure_list("hello"))
        self.assertEqual([], utils.ensure_list("   "))
        self.assertEqual([], utils.ensure_list(None))
        self.assertEqual([], utils.ensure_list([]))

    def test_digest_asi_line(self):
        """Test extracting datetime from ASI log lines"""
        # blank or whitespace-only line
        self.assertIsNone(utils.digest_asi_line(b"   "))

        # PM timestamp
        line_pm = b"10/19/2023 12:00:55 PM"
        expected_pm = datetime(2023, 10, 19, 0, 0, 55)
        self.assertEqual(expected_pm, utils.digest_asi_line(line_pm))

        # AM timestamp
        line_am = b"10/19/2023 01:23:45 AM"
        expected_am = datetime(2023, 10, 19, 1, 23, 45)
        self.assertEqual(expected_am, utils.digest_asi_line(line_am))


if __name__ == "__main__":
    unittest.main()
