""" Unit tests for the behavior_utils module in the utils package. """

import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from aind_metadata_mapper.open_ephys.utils import behavior_utils as behavior


class TestBehaviorUtils(unittest.TestCase):
    """
    Tests Behavior utils
    """

    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".behavior_utils.get_visual_stimuli_df"
    )
    def test_get_stimulus_presentations(self, mock_get_visual_stimuli_df):
        """
        Tests the get_stimulus_presentations function
        """
        data = {}  # Example data, replace with appropriate test data
        stimulus_timestamps = [0.0, 0.5, 1.0, 1.5]

        # Mocking the response of get_visual_stimuli_df
        mock_get_visual_stimuli_df.return_value = pd.DataFrame(
            {
                "frame": [0, 1, 2, 3],
                "time": [0.0, 0.5, 1.0, 1.5],
                "end_frame": [1, 2, 3, np.nan],
            }
        )

        # Expected DataFrame after processing
        expected_df = pd.DataFrame(
            {
                "end_frame": [1, 2, 3, np.nan],
                "start_frame": [0, 1, 2, 3],
                "start_time": [0.0, 0.5, 1.0, 1.5],
                "stop_time": [0.5, 1.0, 1.5, float("nan")],
            },
            index=pd.Index([0, 1, 2, 3], name="stimulus_presentations_id"),
        )

        # Call the function to test
        result_df = behavior.get_stimulus_presentations(
            data, stimulus_timestamps
        )

        # Assert DataFrame equality
        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False, check_index_type=False
        )

    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".behavior_utils.get_images_dict"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".behavior_utils.stim.convert_filepath_caseinsensitive"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".behavior_utils.stim.get_image_set_name"
    )
    def test_get_stimulus_metadata(
        self,
        mock_get_image_set_name,
        mock_convert_filepath_caseinsensitive,
        mock_get_images_dict,
    ):
        """
        Tests the get_stimulus_metadata function
        """
        # Example pkl input
        pkl = {
            "items": {
                "behavior": {
                    "stimuli": {
                        "images": {},
                        "grating": {
                            "phase": 0.5,
                            "sf": 0.03,
                            "set_log": [
                                [0, 0.0],
                                [1, 45.0],
                                [2, 90.0],
                                [3, 0.0],
                            ],
                        },
                    }
                }
            }
        }

        # Mock the get_images_dict function
        mock_get_images_dict.return_value = {
            "metadata": {"image_set": "path/to/images.pkl"},
            "image_attributes": [
                {
                    "image_category": "image",
                    "image_name": "image1.jpg",
                    "orientation": np.NaN,
                    "phase": np.NaN,
                    "spatial_frequency": np.NaN,
                    "image_index": 0,
                },
                {
                    "image_category": "image",
                    "image_name": "image2.jpg",
                    "orientation": np.NaN,
                    "phase": np.NaN,
                    "spatial_frequency": np.NaN,
                    "image_index": 1,
                },
            ],
        }

        # Mock the stim.convert_filepath_caseinsensitive function
        mock_convert_filepath_caseinsensitive.return_value = (
            "path/to/images.pkl"
        )

        # Mock the stim.get_image_set_name function
        mock_get_image_set_name.return_value = "image_set_name"

        # Expected DataFrame
        expected_df = pd.DataFrame(
            {
                "image_index": [0, 1, 2],
                "image_category": ["image", "image", "omitted"],
                "image_name": ["image1.jpg", "image2.jpg", "omitted"],
                "orientation": [np.NaN, np.NaN, np.NaN],
                "phase": [np.NaN, np.NaN, np.NaN],
                "spatial_frequency": [np.NaN, np.NaN, np.NaN],
                "image_set": ["image_set_name", "image_set_name", "omitted"],
                "size": [np.NaN, np.NaN, np.NaN],
            }
        ).set_index("image_index")

        # Call the function
        result_df = behavior.get_stimulus_metadata(pkl)

        # Assert DataFrame equality
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_get_stimulus_epoch(self):
        """
        Tests the get_stimulus_epoch function
        using a fake set_log
        """
        # Example set_log input
        set_log = [
            ("Image", "image1.jpg", 0, 10),
            ("Image", "image2.jpg", 0, 20),
            ("Grating", 45, 0, 30),
        ]
        n_frames = 40

        # Test case where current_set_index is not the last one
        current_set_index = 0
        start_frame = 10
        expected_output = (10, 20)
        result = behavior.get_stimulus_epoch(
            set_log, current_set_index, start_frame, n_frames
        )
        self.assertEqual(result, expected_output)

        # Test case where current_set_index is the last one
        current_set_index = 2
        start_frame = 30
        expected_output = (30, 40)
        result = behavior.get_stimulus_epoch(
            set_log, current_set_index, start_frame, n_frames
        )
        self.assertEqual(result, expected_output)

        # Test case where there is only one stimulus in set_log
        set_log_single = [("Image", "image1.jpg", 0, 10)]
        current_set_index = 0
        start_frame = 10
        expected_output = (10, 40)
        result = behavior.get_stimulus_epoch(
            set_log_single, current_set_index, start_frame, n_frames
        )
        self.assertEqual(result, expected_output)

    def test_get_draw_epochs(self):
        """
        Creats fake draw logs
        tests the get_draw_epochs function
        """
        # Example draw_log input
        draw_log = [0, 0, 1, 1, 0, 1, 1, 1, 0, 0, 1, 1]
        start_frame = 2
        stop_frame = 11

        # Expected output
        expected_output = [(2, 4), (5, 8), (10, 11)]

        # Call the function
        result = behavior.get_draw_epochs(draw_log, start_frame, stop_frame)

        # Assert equality
        self.assertEqual(result, expected_output)

        # Test case where no frames are active
        draw_log_no_active = [0, 0, 0, 0, 0]
        start_frame = 0
        stop_frame = 4
        expected_output_no_active = []
        result_no_active = behavior.get_draw_epochs(
            draw_log_no_active, start_frame, stop_frame
        )
        self.assertEqual(result_no_active, expected_output_no_active)

        # Test case where all frames are active
        draw_log_all_active = [1, 1, 1, 1, 1]
        start_frame = 0
        stop_frame = 4
        expected_output_all_active = [(0, 4)]
        result_all_active = behavior.get_draw_epochs(
            draw_log_all_active, start_frame, stop_frame
        )
        self.assertEqual(result_all_active, expected_output_all_active)

        # Test case with mixed active and inactive frames
        draw_log_mixed = [1, 0, 1, 0, 1, 0, 1]
        start_frame = 0
        stop_frame = 6
        expected_output_mixed = [(0, 1), (2, 3), (4, 5)]
        result_mixed = behavior.get_draw_epochs(
            draw_log_mixed, start_frame, stop_frame
        )
        self.assertEqual(result_mixed, expected_output_mixed)

    def test_unpack_change_log(self):
        """
        Tests changing of the log using names with .jpg
        """
        # Example change input
        change = (("Image", "image1.jpg"), ("Grating", "45_deg"), 12345, 67)

        # Expected output
        expected_output = {
            "frame": 67,
            "time": 12345,
            "from_category": "Image",
            "to_category": "Grating",
            "from_name": "image1.jpg",
            "to_name": "45_deg",
        }

        # Call the function
        result = behavior.unpack_change_log(change)

        # Assert equality
        self.assertEqual(result, expected_output)

        # Test with different data
        change2 = (
            ("Video", "video1.mp4"),
            ("Static", "static_image"),
            54321,
            89,
        )

        expected_output2 = {
            "frame": 89,
            "time": 54321,
            "from_category": "Video",
            "to_category": "Static",
            "from_name": "video1.mp4",
            "to_name": "static_image",
        }

        result2 = behavior.unpack_change_log(change2)
        self.assertEqual(result2, expected_output2)

    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".behavior_utils.get_stimulus_epoch"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils.get_draw_epochs"
    )
    def test_get_visual_stimuli_df(
        self, mock_get_draw_epochs, mock_get_stimulus_epoch
    ):
        """
        Tests the get_visual_stimuli_df function
        Mocks some intermediary functions
        """
        # Mock input data
        mock_data = {
            "items": {
                "behavior": {
                    "stimuli": {
                        "stimulus1": {
                            "set_log": [
                                ("ori", 90, None, 10),
                                ("image", "img1.jpg", None, 20),
                            ],
                            "draw_log": [(5, 15), (25, 35)],
                        },
                        "stimulus2": {
                            "set_log": [
                                ("ori", 270, None, 5),
                                ("image", "img2.jpg", None, 15),
                            ],
                            "draw_log": [(0, 10), (20, 30)],
                        },
                    },
                    "omitted_flash_frame_log": {"omitted_flash1": [1, 2]},
                }
            }
        }

        mock_time = np.arange(3) * 0.1  # Adjust the number of timestamps here

        # Mock return values for get_stimulus_epoch and get_draw_epochs
        mock_get_stimulus_epoch.side_effect = lambda *args, **kwargs: (
            0,
            2,
        )  # Mocking epoch start and end
        mock_get_draw_epochs.side_effect = lambda *args, **kwargs: [
            (0, 2)
        ]  # Mocking draw epochs

        # Call the function under test
        result_df = behavior.get_visual_stimuli_df(mock_data, mock_time)

        # Define expected output dataframe
        expected_columns = [
            "orientation",
            "image_name",
            "frame",
            "end_frame",
            "time",
            "duration",
            "omitted",
        ]
        expected_data = {
            "orientation": [90, 90, 270, 270, np.nan, np.nan],
            "image_name": [
                "img1.jpg",
                "img1.jpg",
                "img2.jpg",
                "img2.jpg",
                "omitted",
                "omitted",
            ],
            "frame": [0, 20, 5, 25, 3, 8],
            "end_frame": [10, 30, 10, 30, np.nan, np.nan],
            "time": [
                mock_time[0],
                mock_time[2],
                mock_time[1],
                mock_time[2],
                mock_time[0],
                mock_time[1],
            ],
            "duration": [
                mock_time[1] - mock_time[0],
                mock_time[2] - mock_time[1],
                mock_time[1] - mock_time[0],
                mock_time[2] - mock_time[1],
                0.25,
                0.25,
            ],
            "omitted": [False, False, False, False, True, True],
        }
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        # Perform assertions
        self.assertEqual(result_df["time"].all(), expected_df["time"].all())

    def test_get_image_names(self):
        """
        Tests the get_image_names function
        """
        # Mock data
        behavior_stimulus_file = {
            "stimuli": {
                "stim1": {
                    "set_log": [
                        ("image", "image1.jpg", None, 0),
                        ("ori", 45, None, 1),
                    ]
                },
                "stim2": {
                    "set_log": [
                        ("image", "image2.jpg", None, 2),
                        ("ori", 90, None, 3),
                    ]
                },
                "stim3": {
                    "set_log": [
                        ("image", "image1.jpg", None, 4),
                        ("ori", 135, None, 5),
                    ]
                },
            }
        }

        # Expected output
        expected_output = {"image1.jpg", "image2.jpg"}

        # Call the function
        result = behavior.get_image_names(behavior_stimulus_file)

        # Assert equality
        self.assertEqual(result, expected_output)

        # Test case with no images
        behavior_stimulus_file_no_images = {
            "stimuli": {
                "stim1": {"set_log": [("ori", 45, None, 1)]},
                "stim2": {"set_log": [("ori", 90, None, 3)]},
            }
        }

        # Expected output
        expected_output_no_images = set()

        # Call the function
        result_no_images = behavior.get_image_names(
            behavior_stimulus_file_no_images
        )

        # Assert equality
        self.assertEqual(result_no_images, expected_output_no_images)

    def test_is_change_event(self):
        """
        Tests the is_change_event function
        """
        # Mock data
        stimulus_presentations = pd.DataFrame(
            {
                "image_name": [
                    "img1",
                    "img1",
                    "img2",
                    "img2",
                    "img3",
                    "omitted",
                    "img3",
                    "img4",
                ],
                "omitted": [
                    False,
                    False,
                    False,
                    False,
                    False,
                    True,
                    False,
                    False,
                ],
            }
        )

        # Expected output
        expected_output = pd.Series(
            [False, False, True, False, True, False, False, True],
            name="is_change",
        )

        # Call the function
        result = behavior.is_change_event(stimulus_presentations)

        # Assert equality
        pd.testing.assert_series_equal(result, expected_output)

    def test_get_flashes_since_change(self):
        """
        Tests the get_flashes_since_change function
        """
        # Mock data
        stimulus_presentations = pd.DataFrame(
            {
                "image_name": [
                    "img1",
                    "img1",
                    "img2",
                    "img2",
                    "img3",
                    "omitted",
                    "img3",
                    "img4",
                ],
                "omitted": [
                    False,
                    False,
                    False,
                    False,
                    False,
                    True,
                    False,
                    False,
                ],
                "is_change": [
                    False,
                    False,
                    True,
                    False,
                    True,
                    False,
                    False,
                    True,
                ],
            }
        )

        # Expected output
        expected_output = pd.Series(
            [0, 1, 0, 1, 0, 0, 1, 0], name="flashes_since_change"
        )

        # Call the function
        result = behavior.get_flashes_since_change(stimulus_presentations)

        # Assert equality
        pd.testing.assert_series_equal(
            result, expected_output, check_dtype=False
        )

    def test_add_active_flag(self):
        """
        Tests the add_active_flag function
        """
        # Mock data for stimulus presentations table
        stim_pres_table = pd.DataFrame(
            {
                "start_time": [1, 5, 10, 15, 20, 25, 30],
                "stop_time": [2, 6, 11, 16, 21, 26, 31],
                "image_name": [
                    "img1",
                    "img2",
                    "img3",
                    np.nan,
                    "img4",
                    "img5",
                    "img6",
                ],
                "stimulus_block": [1, 1, 2, 2, 3, 3, 3],
            }
        )

        # Mock data for trials table
        trials = pd.DataFrame({"start_time": [0, 10], "stop_time": [20, 40]})

        # Expected output
        expected_active = pd.Series(
            [True, True, True, True, True, True, True], name="active"
        )
        expected_output = stim_pres_table.copy()
        expected_output["active"] = expected_active

        # Call the function
        result = behavior.add_active_flag(stim_pres_table, trials)

        # Assert the 'active' column is correctly added
        pd.testing.assert_series_equal(result["active"], expected_active)

    def test_compute_trials_id_for_stimulus(self):
        """
        Tests the compute_trials_id_for_stimulus function
        """
        # Mock data for stimulus presentations table
        stim_pres_table = pd.DataFrame(
            {
                "start_time": [1, 5, 10, 15, 20, 25, 30, 35, 40, 45],
                "stop_time": [2, 6, 11, 16, 21, 26, 31, 36, 41, 46],
                "image_name": [
                    "img1",
                    "img2",
                    "img3",
                    np.nan,
                    "img4",
                    "img5",
                    "img6",
                    "img1",
                    "img2",
                    "img3",
                ],
                "stimulus_block": [1, 1, 2, 2, 3, 3, 3, 4, 4, 4],
                "active": [
                    True,
                    True,
                    True,
                    True,
                    False,
                    False,
                    False,
                    True,
                    True,
                    True,
                ],
            }
        )

        # Mock data for trials table
        trials_table = pd.DataFrame(
            {"start_time": [0, 10], "stop_time": [20, 40]}
        )

        # Expected output
        expected_trials_id = pd.Series(
            data=[0, 0, 0, -99, 1, 1, 1, 1, -99, -99],
            index=stim_pres_table.index,
            name="trials_id",
        ).astype("int")

        # Call the function
        result = behavior.compute_trials_id_for_stimulus(
            stim_pres_table, trials_table
        )

        # Assert the trials_id series is correctly assigned
        pd.testing.assert_series_equal(result, expected_trials_id)

    def test_fix_omitted_end_frame(self):
        """
        Tests the fix_omitted_end_frame function
        """
        # Mock data for stimulus presentations table
        stim_pres_table = pd.DataFrame(
            {
                "start_frame": [0, 5, 10, 15, 20],
                "end_frame": [5, 10, 15, np.nan, 25],
                "omitted": [False, False, False, True, False],
            }
        )

        # Calculate expected median stimulus frame duration
        median_stim_frame_duration = np.nanmedian(
            stim_pres_table["end_frame"] - stim_pres_table["start_frame"]
        )

        # Expected output
        expected_end_frame = stim_pres_table["end_frame"].copy()
        expected_end_frame.iloc[3] = (
            stim_pres_table["start_frame"].iloc[3] + median_stim_frame_duration
        )

        expected_stim_pres_table = stim_pres_table.copy()
        expected_stim_pres_table["end_frame"] = expected_end_frame
        expected_stim_pres_table = expected_stim_pres_table.astype(
            {"start_frame": int, "end_frame": int}
        )

        # Call the function
        result = behavior.fix_omitted_end_frame(stim_pres_table)

        # Assert the DataFrame is correctly modified
        pd.testing.assert_frame_equal(result, expected_stim_pres_table)

    def test_compute_is_sham_change_no_column(self):
        """
        tests the compute_is_sham_change function
        """
        stim_df_no_active = pd.DataFrame(
            {
                "trials_id": [0, 0, 0, 1, 1, 1],
                "stimulus_block": [1, 1, 2, 2, 3, 3],
                "image_name": ["A", "A", "B", "B", "C", "C"],
                "start_frame": [0, 10, 20, 30, 40, 50],
                "is_sham_change": [False, False, False, False, False, False],
            }
        )

        # Create a sample trials DataFrame
        trials = pd.DataFrame(
            {"catch": [False, False, True], "change_frame": [10, 40, 60]}
        )

        expected_stim_df = stim_df_no_active.copy()

        result = behavior.compute_is_sham_change(stim_df_no_active, trials)

        pd.testing.assert_frame_equal(result, expected_stim_df)

    def test_fingerprint_from_stimulus_file(self):
        """
        Creates a fake stim file and
        Tests the fingerprint_from_stimulus_file function
        """
        stimulus_presentations = pd.DataFrame(
            {
                "stim_block": [1, 1, 2, 2],
            }
        )

        stimulus_file = {
            "items": {
                "behavior": {
                    "items": {
                        "fingerprint": {
                            "static_stimulus": {
                                "runs": 3,
                                "frame_list": [0, 1, 1, 0, 1, 1],
                                "sweep_frames": [[0, 1], [2, 3], [4, 5]],
                                "sweep_order": [0, 1, 2],
                                "sweep_table": [[3], [4], [5]],
                                "dimnames": [
                                    "movie_frame_index",
                                    "movie_repeat",
                                ],
                            },
                            "frame_indices": [0, 1, 2, 3, 4, 5],
                        }
                    }
                }
            }
        }

        stimulus_timestamps = [0, 1, 2, 3, 4, 5]
        # Call the function under test
        result = behavior.fingerprint_from_stimulus_file(
            stimulus_presentations,
            pd.DataFrame(stimulus_file),
            stimulus_timestamps,
            "fingerprint",
        )

        # Define expected output based on the provided mock data
        expected_columns = [
            "movie_frame_index",
            "start_time",
            "stop_time",
            "start_frame",
            "end_frame",
            "movie_repeat",
            "duration",
            "stim_block",
            "stim_name",
        ]

        expected_data = [
            {
                "movie_frame_index": 3,
                "start_time": 0,
                "stop_time": 1,
                "start_frame": 0,
                "end_frame": 1,
                "movie_repeat": 0,
                "duration": 1,
                "stim_block": 3,
                "stim_name": "natural_movie_one",
            },
            {
                "movie_frame_index": 4,
                "start_time": 0,
                "stop_time": 1,
                "start_frame": 4,
                "end_frame": 5,
                "movie_repeat": 1,
                "duration": 1,
                "stim_block": 3,
                "stim_name": "natural_movie_one",
            },
            {
                "movie_frame_index": 5,
                "start_time": 0,
                "stop_time": 1,
                "start_frame": 8,
                "end_frame": 9,
                "movie_repeat": 2,
                "duration": 1,
                "stim_block": 3,
                "stim_name": "natural_movie_one",
            },
        ]

        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        # Assert that the result matches the expected DataFrame
        self.assertEqual(
            expected_df["movie_frame_index"].values.tolist(),
            result["movie_frame_index"].values.tolist(),
        )

    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.load_pkl")
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".get_stimulus_presentations"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".check_for_errant_omitted_stimulus"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".get_stimulus_metadata"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".is_change_event"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".get_flashes_since_change"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".get_stimulus_name"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".fix_omitted_end_frame"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils.behavior_utils"
        ".add_fingerprint_stimulus"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils" ".behavior_utils.postprocess"
    )
    def test_from_stimulus_file(
        self,
        mock_postprocess,
        mock_add_fingerprint_stimulus,
        mock_fix_omitted_end_frame,
        mock_get_stimulus_name,
        mock_get_flashes_since_change,
        mock_is_change_event,
        mock_get_stimulus_metadata,
        mock_check_for_errant_omitted_stimulus,
        mock_get_stimulus_presentations,
        mock_load_pkl,
    ):
        """
        Tests the from_stimulus_file function
        mocks intermediary functions so the test
        isn't 1000 lines
        """
        # Mock data
        stimulus_file = MagicMock()
        stimulus_timestamps = MagicMock()
        limit_to_images = ["image1", "image2"]
        column_list = ["column1", "column2"]
        project_code = "VBO"

        # Mock return values
        mock_load_pkl.return_value = MagicMock()
        mock_get_stimulus_presentations.return_value = pd.DataFrame(
            {
                "start_time": [0, 1, 2],
                "image_name": ["image1", "image2", "image1"],
                "orientation": [0, 90, 180],
                "index": ["0", "oris", "phase"],
            }
        )
        mock_check_for_errant_omitted_stimulus.return_value = pd.DataFrame(
            {
                "start_time": [0, 1, 2],
                "image_name": ["image1", "image2", "image1"],
                "orientation": [0, 90, 180],
                "index": ["0", "oris", "phase"],
            }
        )
        mock_get_stimulus_metadata.return_value = pd.DataFrame(
            {
                "image_name": ["image1", "image2"],
                "image_set": ["set1", "set2"],
                "image_index": [1, 2],
                "start_time": [0, 1],
                "phase": ["A", "B"],
                "spatial_frequency": [1.0, 2.0],
                "index": ["A", "phase"],
            }
        )
        mock_is_change_event.return_value = pd.Series([True, False, True])
        mock_get_flashes_since_change.return_value = pd.Series([0, 1, 0])
        mock_get_stimulus_name.return_value = "natural_movie_one"
        mock_fix_omitted_end_frame.return_value = pd.DataFrame(
            {
                "start_frame": [0, 1, 2],
                "end_frame": [1, 3, 4],
                "omitted": [False, False, False],
                "index": ["A", "B", "phase"],
            }
        )
        mock_add_fingerprint_stimulus.return_value = pd.DataFrame(
            {
                "start_time": [0, 1, 2],
                "end_time": [1, 2, 3],
                "image_name": ["image1", "image2", "image1"],
                "is_change": [True, False, True],
                "stim_block": [1, 1, 2],
                "index": ["A", "B", "phase"],
            }
        )
        mock_postprocess.return_value = pd.DataFrame(
            {
                "start_time": [0, 1, 2],
                "end_time": [1, 2, 3],
                "image_name": ["image1", "image2", "image1"],
                "is_change": [True, False, True],
                "stim_block": [1, 1, 2],
                "index": ["A", "B", "phase"],
            }
        )

        # Call the function under test
        result = behavior.from_stimulus_file(
            stimulus_file,
            stimulus_timestamps,
            limit_to_images,
            column_list,
            project_code=project_code,
        )

        # Define expected output based on the mocked return values
        expected_columns = [
            "start_time",
            "end_time",
            "image_name",
            "is_change",
            "stim_block",
            "stim_name",
            "movie_frame_index",
            "movie_repeat",
            "duration",
            "flashes_since_change",
        ]

        expected_data = {
            "start_time": [0, 1, 2],
            "end_time": [1, 2, 3],
            "image_name": ["image1", "image2", "image1"],
            "is_change": [True, False, True],
            "stim_block": [1, 1, 2],
            "stim_name": "natural_movie_one",
            "movie_frame_index": [0, 0, 0],
            "movie_repeat": [0, 0, 1],
            "duration": [1, 1, 1],
            "flashes_since_change": [0, 1, 0],
        }

        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        # Assert that the result matches the expected DataFrame
        self.assertEqual(
            expected_df["start_time"].all(), result["start_time"].all()
        )

    def test_postprocess(self):
        """
        Tests the postprocess function
        """
        # Actual input data
        presentations = pd.DataFrame(
            {
                "image_name": ["image1", "image2", "image3", None],
                "omitted": [False, True, False, False],
                "duration": [0.25, None, None, None],
                "boolean_col": [True, False, True, False],
                "object_col": [True, None, False, None],
                "start_time": [0, 1, 2, 3],
            }
        )

        # Call the function under test
        processed_presentations = behavior.postprocess(presentations)

        expected_columns = [
            "start_time",
            "stop_time",
            "image_name",
            "omitted",
            "duration",
            "boolean_col",
            "object_col",
        ]
        expected_data = {
            "image_name": ["image1", "image2", "image3", None],
            "omitted": [False, True, False, False],
            "duration": [
                0.25,
                0.25,
                0.25,
                0.25,
            ],  # Example of filled omitted values
            "boolean_col": [True, False, True, False],
            "object_col": [True, None, False, None],
            "start_time": [0, 1, 2, 3],
            "stop_time": [None, 1.25, None, None],
        }
        expected_df = pd.DataFrame(expected_data, columns=expected_columns)

        processed_presentations = pd.DataFrame(processed_presentations)
        # Assert that the result matches the expected DataFrame
        self.assertEqual(
            expected_df["duration"].all(),
            processed_presentations["duration"].all(),
        )
        self.assertEqual(
            expected_df["start_time"].all(),
            processed_presentations["start_time"].all(),
        )
        self.assertEqual(
            expected_df["image_name"].all(),
            processed_presentations["image_name"].all(),
        )
        self.assertEqual(
            expected_df["omitted"].all(),
            processed_presentations["omitted"].all(),
        )
        self.assertEqual(
            expected_df["boolean_col"].all(),
            processed_presentations["boolean_col"].all(),
        )

    def test_check_for_errant_omitted_stimulus(self):
        """
        Tests the check_for_errant_omitted_stimulus function
        """
        # Actual input data
        data = {
            "omitted": [True, False, False, False],
            "stimulus_block": [1, 1, 2, 2],
            "other_column": [1, 2, 3, 4],
        }
        input_df = pd.DataFrame(data)

        # Call the function under test
        processed_df = behavior.check_for_errant_omitted_stimulus(input_df)

        # Define expected output based on the expected behavior of the function
        expected_data = {
            "omitted": [False, False, False],
            "stimulus_block": [1, 2, 2],
            "other_column": [2, 3, 4],
        }
        expected_df = pd.DataFrame(expected_data)
        # Assert that the result matches the expected DataFrame
        self.assertEqual(
            processed_df["omitted"].all(), expected_df["omitted"].all()
        )

    def test_fill_missing_values_for_omitted_flashes(self):
        """
        Tests the fill_missing_values_for_omitted_flashes function
        """
        # Actual input data
        data = {
            "start_time": [0.0, 1.0, 2.0, 3.0],
            "stop_time": [0, 0, 0, 0],
            "duration": [1, 1, 0, 0],
            "omitted": [False, True, False, True],
        }
        df = pd.DataFrame(data)

        # Call the function under test
        processed_df = behavior.fill_missing_values_for_omitted_flashes(
            df, omitted_time_duration=0.25
        )

        # Define expected output based on the expected behavior of the function
        expected_data = {
            "start_time": [0.0, 1.0, 2.0, 3.0],
            "stop_time": [0.0, 1.25, 0.0, 3.25],
            "duration": [1, 0.25, 0.0, 0.25],
            "omitted": [False, True, False, True],
        }
        expected_df = pd.DataFrame(expected_data)

        # Assert that the result matches the expected DataFrame
        pd.testing.assert_frame_equal(processed_df, expected_df)

    def test_get_spontaneous_stimulus(self):
        """
        Tests the get_spontaneous_stimulus function
        """
        # Define a sample stimulus presentations table with gaps
        data = {
            "start_frame": [0, 100, 200, 400, 500],
            "start_time": [0.0, 10.0, 20.0, 40.0, 50.0],
            "end_frame": [100, 200, 300, 500, 600],
            "stop_time": [10.0, 20.0, 30.0, 50.0, 60.0],
            "stim_block": [0, 1, 2, 4, 5],
            "stim_name": ["stim1", "stim2", "stim3", "stim4", "stim5"],
        }
        df = pd.DataFrame(data)

        # Call the function under test
        processed_df = behavior.get_spontaneous_stimulus(df)

        # Define expected output based on the expected behavior of the function
        expected_data = {
            "start_frame": [0, 100, 200, 285, 400, 500],
            "start_time": [0.0, 10.0, 20.0, 285.0, 40.0, 50.0],
            "stop_time": [10.0, 20.0, 30.0, 285.0, 50.0, 60.0],
            "stim_block": [0, 1, 2, 3, 4, 5],
            "stim_name": [
                "spontaneous",
                "stim1",
                "stim2",
                "spontaneous",
                "stim3",
                "stim4",
            ],
        }
        expected_df = pd.DataFrame(expected_data)

        # Assert that the result matches the expected DataFrame
        self.assertEqual(
            processed_df["start_frame"].all(), expected_df["start_frame"].all()
        )

    def test_add_fingerprint_stimulus(self):
        """
        Simulates a fingerprint stim and
        Tests the add_fingerprint_stimulus function
        """
        stimulus_file = {
            "items": {
                "behavior": {
                    "items": {
                        "fingerprint": {
                            "static_stimulus": {
                                "runs": 3,
                                "frame_list": [0, 1, 1, 0, 1, 1],
                                "sweep_frames": [[0, 1], [2, 3], [4, 5]],
                                "sweep_order": [0, 1, 2],
                                "sweep_table": [[3], [4], [5]],
                                "dimnames": [
                                    "movie_frame_index",
                                    "movie_repeat",
                                ],
                            },
                            "frame_indices": [0, 1, 2, 3, 4, 5],
                        }
                    }
                }
            }
        }

        stimulus_presentations = pd.DataFrame(
            {
                "stim_block": [0, 0, 0, 1, 1, 1],
                "start_time": [0.0, 1.0, 2.0, 5.0, 6.0, 7.0],
                "stop_time": [0.5, 1.5, 2.5, 5.5, 6.5, 7.5],
                "start_frame": [0, 1, 2, 5, 6, 7],
                "end_frame": [0, 1, 2, 5, 6, 7],
            }
        )  # Mock the stimulus file as needed
        stimulus_timestamps = np.array([0.0, 10.0, 20.0, 30.0, 40.0])

        # Call the function under test
        processed_df = behavior.add_fingerprint_stimulus(
            stimulus_presentations=stimulus_presentations,
            stimulus_file=stimulus_file,
            stimulus_timestamps=stimulus_timestamps,
            fingerprint_name="fingerprint",
        )

        # Define expected output based on the expected behavior of the function
        expected_data = {
            "start_frame": [0, 100, 200, 300, 400, 500],
            "start_time": [0.0, 10.0, 20.0, 30.0, 40.0, 285.0],
            "stop_time": [10.0, 20.0, 30.0, 40.0, 285.0, 300.0],
            "stim_block": [0, 1, 2, 3, 4, 5],
            "stim_name": [
                "stim1",
                "stim2",
                "stim3",
                "stim4",
                "spontaneous",
                "fingerprint",
            ],
        }
        expected_df = pd.DataFrame(expected_data)

        # Assert that the result matches the expected DataFrame
        self.assertEqual(
            processed_df["start_frame"].all(), expected_df["start_frame"].all()
        )

    def test_get_spontaneous_block_indices(self):
        """
        Tests the get_spontaneous_block_indices function
        """
        # Test case 1: No gaps between stimulus blocks
        stimulus_blocks1 = np.array([0, 1, 2, 3])
        expected_indices1 = np.array([], dtype=np.int64)
        np.testing.assert_array_equal(
            behavior.get_spontaneous_block_indices(stimulus_blocks1),
            expected_indices1,
        )

        # Test case 2: Single gap between stimulus blocks
        stimulus_blocks2 = np.array([0, 2, 3])
        expected_indices2 = np.array([1], dtype=np.int64)
        np.testing.assert_array_equal(
            behavior.get_spontaneous_block_indices(stimulus_blocks2),
            expected_indices2,
        )

        # Test case 4: No spontaneous blocks (no gaps)
        stimulus_blocks4 = np.array([0, 1, 2, 3, 4])
        expected_indices4 = np.array([], dtype=np.int64)
        np.testing.assert_array_equal(
            behavior.get_spontaneous_block_indices(stimulus_blocks4),
            expected_indices4,
        )

        # Test case 5: Raises RuntimeError for large gap
        stimulus_blocks5 = np.array([0, 3, 4, 5])
        with self.assertRaises(RuntimeError):
            behavior.get_spontaneous_block_indices(stimulus_blocks5)
