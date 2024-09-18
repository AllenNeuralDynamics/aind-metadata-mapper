""" Unit tests for the pkl_utils module. """

import unittest
import numpy as np

from aind_metadata_mapper.open_ephys.utils import pkl_utils as pkl


class TestPKL(unittest.TestCase):
    """
    Test class for the pkl_utils module.
    """

    def test_get_stimuli(self):
        """
        Creating a sample pkl dictionary with a "stimuli" key
        """
        sample_pkl = {
            "stimuli": ["image1.jpg", "image2.jpg", "image3.jpg"],
            "other_key": "other_value",
        }

        # Calling the function with the sample pkl dictionary
        result = pkl.get_stimuli(sample_pkl)

        # Asserting that the result is the "stimuli" key
        self.assertEqual(result, sample_pkl["stimuli"])

    def test_get_stimuli_missing_key(self):
        """
        Creating a sample pkl dictionary without a "stimuli" key
        """
        sample_pkl = {"other_key": "other_value"}

        # Asserting that accessing the "stimuli" key raises a KeyError
        with self.assertRaises(KeyError):
            pkl.get_stimuli(sample_pkl)

    def test_get_fps(self):
        """
        Test the get_fps function
        """
        # Creating a sample pkl dictionary with a "fps" key
        sample_pkl = {"fps": 30, "other_key": "other_value"}

        # Calling the function with the sample pkl dictionary
        result = pkl.get_fps(sample_pkl)

        # Asserting that the result is the value associated with the "fps" key
        self.assertEqual(result, sample_pkl["fps"])

    def test_get_fps_missing_key(self):
        """
        Test the get_fps function with a missing key
        """
        # Creating a sample pkl dictionary without a "fps" key
        sample_pkl = {"other_key": "other_value"}

        # Asserting that accessing the "fps" key raises a KeyError
        with self.assertRaises(KeyError):
            pkl.get_fps(sample_pkl)

    def test_get_pre_blank_sec(self):
        """
        Test the get_pre_blank_sec function
        """
        # Creating a sample pkl dictionary with a "pre_blank_sec" key
        sample_pkl = {"pre_blank_sec": 2, "other_key": "other_value"}

        # Calling the function with the sample pkl dictionary
        result = pkl.get_pre_blank_sec(sample_pkl)

        # Asserting that the result is the "pre_blank_sec" key
        self.assertEqual(result, sample_pkl["pre_blank_sec"])

    def test_get_pre_blank_sec_missing_key(self):
        """
        Test the get_pre_blank_sec function with a missing key
        """

        # Creating a sample pkl dictionary without a "pre_blank_sec" key
        sample_pkl = {"other_key": "other_value"}

        # Asserting that accessing the "pre_blank_sec" key raises a KeyError
        with self.assertRaises(KeyError):
            pkl.get_pre_blank_sec(sample_pkl)

    def test_get_running_array(self):
        """
        Test the get_running_array function
        """
        # Creating a sample pkl dictionary with a nested structure
        sample_pkl = {
            "items": {"foraging": {"encoders": [{"dx": [1, 2, 3, 4]}]}},
            "other_key": "other_value",
        }

        # Calling the function with the sample pkl dictionary and the key "dx"
        result = pkl.get_running_array(sample_pkl, "dx")

        # Asserting that the result is the expected numpy array
        np.testing.assert_array_equal(result, np.array([1, 2, 3, 4]))

    def test_get_running_array_missing_key(self):
        """
        Tests the get_running_array function with a missing key
        """
        # Creating a sample pkl dictionary without the nested "dx" key
        sample_pkl = {
            "items": {"foraging": {"encoders": [{"dy": [1, 2, 3, 4]}]}},
            "other_key": "other_value",
        }

        # Asserting that accessing the "dx" key raises a KeyError
        with self.assertRaises(KeyError):
            pkl.get_running_array(sample_pkl, "dx")

    def test_get_angular_wheel_rotation(self):
        """
        Test the get_angular_wheel_rotation function
        """
        # Creating a sample pkl dictionary with a nested "dx" key
        sample_pkl = {
            "items": {"foraging": {"encoders": [{"dx": [5, 6, 7, 8]}]}},
            "other_key": "other_value",
        }

        # Calling the function with the sample pkl dictionary
        result = pkl.get_angular_wheel_rotation(sample_pkl)

        # Asserting that the result is the expected numpy array
        np.testing.assert_array_equal(result, np.array([5, 6, 7, 8]))

    def test_angular_wheel_velocity(self):
        """
        Test the angular_wheel_velocity function
        """
        # Creating a sample pkl dictionary with "fps" and nested "dx" key
        sample_pkl = {
            "fps": 2,
            "items": {"foraging": {"encoders": [{"dx": [2, 3]}]}},
            "other_key": "other_value",
        }

        # Calling the function with the sample pkl dictionary
        result = pkl.angular_wheel_velocity(sample_pkl)

        # Asserting that the result is the expected numpy array
        np.testing.assert_array_equal(result, np.array([4, 6]))

    def test_vsig(self):
        """
        Test the vsig function
        """
        # Creating a sample pkl dictionary with a nested "vsig" key
        sample_pkl = {
            "items": {"foraging": {"encoders": [{"vsig": [1.1, 2.2, 3.3]}]}},
            "other_key": "other_value",
        }

        # Calling the function with the sample pkl dictionary
        result = pkl.vsig(sample_pkl)

        # Asserting that the result is the expected numpy array
        np.testing.assert_array_equal(result, np.array([1.1, 2.2, 3.3]))

    def test_vin(self):
        """
        Test the vin function
        """
        # Creating a sample pkl dictionary with a nested "vin" key
        sample_pkl = {
            "items": {"foraging": {"encoders": [{"vin": [0.5, 1.5, 2.5]}]}},
            "other_key": "other_value",
        }

        # Calling the function with the sample pkl dictionary
        result = pkl.vin(sample_pkl)

        # Asserting that the result is the expected numpy array
        np.testing.assert_array_equal(result, np.array([0.5, 1.5, 2.5]))


if __name__ == "__main__":
    unittest.main()
