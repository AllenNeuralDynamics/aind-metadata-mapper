""" Unit tests for the stim_utils module in the utils package. """

import re
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from aind_metadata_mapper.open_ephys.utils import stim_utils as stim


class TestStimUtils(unittest.TestCase):
    """
    Tests Stim utils
    """

    def test_convert_filepath_caseinsensitive(self):
        """
        Test the convert_filepath_caseinsensitive function.
        """
        # Test when "TRAINING" is in the filename
        self.assertEqual(
            stim.convert_filepath_caseinsensitive("some/TRAINING/file.txt"),
            "some/training/file.txt",
        )

        # Test when "TRAINING" is not in the filename
        self.assertEqual(
            stim.convert_filepath_caseinsensitive("some/OTHER/file.txt"),
            "some/OTHER/file.txt",
        )

        # Test when "TRAINING" is in the middle of the filename
        self.assertEqual(
            stim.convert_filepath_caseinsensitive(
                "some/TRAINING/file/TRAINING.txt"
            ),
            "some/training/file/training.txt",
        )

        # Test when "TRAINING" is at the end of the filename
        self.assertEqual(
            stim.convert_filepath_caseinsensitive("some/file/TRAINING"),
            "some/file/training",
        )

        # Test when filename is empty
        self.assertEqual(stim.convert_filepath_caseinsensitive(""), "")

        # Test when filename is just "TRAINING"
        self.assertEqual(
            stim.convert_filepath_caseinsensitive("TRAINING"), "training"
        )

    def test_enforce_df_int_typing(self):
        """
        Test the enforce_df_int_typing function.
        """

        # Create a sample DataFrame
        df = pd.DataFrame(
            {
                "A": [1, 2, 3, None],
                "B": [4, None, 6, 7],
            }
        )

        # Expected DataFrame using pandas Int64 type
        expected_df_pandas_type = pd.DataFrame(
            {
                "A": [1, 2, 3, pd.NA],
                "B": [4, pd.NA, 6, 7],
            },
            dtype="Int64",
        )

        # Test using pandas Int64 type
        result_df_pandas_type = stim.enforce_df_int_typing(
            df.copy(), ["A", "B"], use_pandas_type=True
        )
        pd.testing.assert_frame_equal(
            result_df_pandas_type, expected_df_pandas_type
        )

    def test_enforce_df_column_order(self):
        """
        Test the enforce_df_column_order function.
        """
        # Create a sample DataFrame
        df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9], "D": [10, 11, 12]}
        )

        # Test case: Specified column order
        column_order = ["D", "B", "C", "A"]
        expected_df = pd.DataFrame(
            {
                "D": [10, 11, 12],
                "B": [4, 5, 6],
                "C": [7, 8, 9],
                "A": [1, 2, 3],
            }
        )
        result_df = stim.enforce_df_column_order(df, column_order)
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Test case: Specified column order with non-existing columns
        column_order = ["D", "E", "B"]
        expected_df = pd.DataFrame(
            {
                "D": [10, 11, 12],
                "B": [4, 5, 6],
                "C": [7, 8, 9],
                "A": [1, 2, 3],
            }
        )
        result_df = stim.enforce_df_column_order(df, column_order)
        pd.testing.assert_frame_equal(result_df, expected_df, check_like=True)

        # Test case: Specified column order with all columns
        column_order = ["C", "A", "D", "B"]
        expected_df = pd.DataFrame(
            {"C": [7, 8, 9], "A": [1, 2, 3], "D": [10, 11, 12], "B": [4, 5, 6]}
        )
        result_df = stim.enforce_df_column_order(df, column_order)
        pd.testing.assert_frame_equal(result_df, expected_df)

        # Test case: Empty DataFrame
        empty_df = pd.DataFrame()
        column_order = ["A", "B"]
        result_df = stim.enforce_df_column_order(empty_df, column_order)
        pd.testing.assert_frame_equal(result_df, empty_df)

    def test_extract_blocks_from_stim(self):
        """
        Creating a sample pkl dictionary with a "stimuli" block key
        """
        sample_pkl = ["image1.jpg", "image2.jpg", "image3.jpg"]

        # Calling the function with the sample pkl dictionary
        result = stim.extract_blocks_from_stim(sample_pkl)

        # Asserting that the result is the "stimuli" key
        self.assertEqual(result, sample_pkl)

    def test_seconds_to_frames(self):
        """
        Test the seconds_to_frames function.
        """

        # Mock data
        seconds = [1.0, 2.5, 3.0]
        pkl_file = "test.pkl"
        pre_blank_sec = 0.5
        fps = 30

        # Expected result
        expected_frames = [45, 90, 105]

        # Mock pkl functions
        with patch(
            "aind_metadata_mapper.open_ephys.utils."
            "stim_utils.pkl.get_pre_blank_sec",
            return_value=pre_blank_sec,
        ):
            with patch(
                "aind_metadata_mapper.open_ephys.utils.stim_utils.pkl.get_fps",
                return_value=fps,
            ):
                result_frames = stim.seconds_to_frames(seconds, pkl_file)
                np.testing.assert_array_equal(result_frames, expected_frames)

    def test_extract_const_params_from_stim_repr(self):
        """
        Test the extract_const_params_from_stim_repr function.
        """

        # Sample input data
        stim_repr = "param1=10, param3='value3', param4=4.5"

        # Mock patterns
        repr_params_re = re.compile(r"(\w+=[^,]+)")
        array_re = re.compile(r"^\[(?P<contents>.*)\]$")

        # Expected result
        expected_params = {"param1": 10, "param3": "value3", "param4": 4.5}

        with patch(
            "aind_metadata_mapper.open_ephys.utils"
            ".stim_utils.ast.literal_eval",
            side_effect=lambda x: eval(x),
        ):
            result_params = stim.extract_const_params_from_stim_repr(
                stim_repr, repr_params_re, array_re
            )
            assert result_params == expected_params

    def test_parse_stim_repr(self):
        """
        Test the parse_stim_repr function.
        """

        # Sample input data
        stim_repr = "param1=10, param2=[1, 2, 3], param3='value3', param4=4.5"
        drop_params = ("param2", "param3")

        # Mock patterns
        repr_params_re = re.compile(r"(\w+=[^,]+)")
        array_re = re.compile(r"^\[(?P<contents>.*)\]$")

        # Mock extract_const_params_from_stim_repr return value
        extracted_params = {
            "param1": 10,
            "param2": [1, 2, 3],
            "param3": "value3",
            "param4": 4.5,
        }

        # Expected result after dropping specified parameters
        expected_params = {"param1": 10, "param4": 4.5}

        with patch(
            "aind_metadata_mapper.open_ephys.utils"
            ".stim_utils.extract_const_params_from_stim_repr",
            return_value=extracted_params,
        ):
            with patch(
                "aind_metadata_mapper.open_ephys.utils.stim_utils.logger"
            ) as mock_logger:
                result_params = stim.parse_stim_repr(
                    stim_repr,
                    drop_params=drop_params,
                    repr_params_re=repr_params_re,
                    array_re=array_re,
                )
                assert result_params == expected_params
                mock_logger.debug.assert_called_with(expected_params)

    def test_create_stim_table(self):
        """
        Test the create_stim_table function.
        """

        # Sample input data
        pkl_file = "test.pkl"
        stimuli = [{"stimulus": "stim1"}, {"stimulus": "stim2"}]

        # Mock stimulus tables
        stim_table_1 = pd.DataFrame(
            {
                "start_time": [10, 20],
                "end_time": [15, 25],
                "stim_param": ["a", "b"],
            }
        )
        stim_table_2 = pd.DataFrame(
            {
                "start_time": [30, 40],
                "end_time": [35, 45],
                "stim_param": ["c", "d"],
            }
        )
        stim_table_3 = pd.DataFrame(
            {
                "start_time": [5, 50],
                "end_time": [10, 55],
                "stim_param": ["e", "f"],
            }
        )

        # Expected full stimulus table
        expected_stim_table_full = pd.DataFrame(
            {
                "start_time": [5, 10, 20, 30, 40, 50],
                "end_time": [10, 15, 25, 35, 45, 55],
                "stim_param": ["e", "a", "b", "c", "d", "f"],
                "stim_index": [pd.NA, 0.0, 0.0, 1.0, 1.0, pd.NA],
                "stim_block": [0, 0, 0, 1, 1, 2],
            }
        )

        # Mock stimulus_tabler function
        def mock_stimulus_tabler(pkl_file, stimulus):
            """
            Mock function for stim intermediary func
            """
            if stimulus["stimulus"] == "stim1":
                return [stim_table_1]
            elif stimulus["stimulus"] == "stim2":
                return [stim_table_2]
            return []

        # Mock spontaneous_activity_tabler function
        def mock_spontaneous_activity_tabler(stimulus_tables):
            """
            Mock of the spontaneous activity tabler
            """
            return [stim_table_3]

        result_stim_table_full = stim.create_stim_table(
            pkl_file,
            stimuli,
            mock_stimulus_tabler,
            mock_spontaneous_activity_tabler,
        )
        self.assertEquals(
            result_stim_table_full["start_time"].all(),
            expected_stim_table_full["start_time"].all(),
        )
        self.assertEquals(
            result_stim_table_full["end_time"].all(),
            expected_stim_table_full["end_time"].all(),
        )
        self.assertEquals(
            result_stim_table_full["stim_param"].all(),
            expected_stim_table_full["stim_param"].all(),
        )
        self.assertEquals(
            result_stim_table_full["stim_block"].all(),
            expected_stim_table_full["stim_block"].all(),
        )

    def test_make_spontaneous_activity_tables(self):
        """
        Test the make_spontaneous_activity_tables function.
        """

        # Sample input data
        stimulus_tables = [
            pd.DataFrame({"start_time": [0, 20], "stop_time": [10, 30]}),
            pd.DataFrame({"start_time": [40, 60], "stop_time": [50, 70]}),
        ]

        # Expected result without duration threshold
        expected_spon_sweeps_no_threshold = pd.DataFrame(
            {"start_time": [30], "stop_time": [40]}
        )

        # Expected result with duration threshold of 10
        expected_spon_sweeps_with_threshold = pd.DataFrame(
            {"start_time": [], "stop_time": []}, dtype="int64"
        )

        # Call the function without duration threshold
        result_no_threshold = stim.make_spontaneous_activity_tables(
            stimulus_tables, duration_threshold=0.0
        )
        pd.testing.assert_frame_equal(
            result_no_threshold[0], expected_spon_sweeps_no_threshold
        )

        # Call the function with duration threshold
        result_with_threshold = stim.make_spontaneous_activity_tables(
            stimulus_tables, duration_threshold=10.0
        )
        pd.testing.assert_frame_equal(
            result_with_threshold[0], expected_spon_sweeps_with_threshold
        )

    def test_extract_frame_times_from_photodiode(self):
        """
        Test the extract_frame_times_from_photodiode function.
        """
        # Sample input data
        sync_file = MagicMock()
        photodiode_cycle = 60
        frame_keys = ("frame_key_1", "frame_key_2")
        photodiode_keys = ("photodiode_key_1", "photodiode_key_2")
        trim_discontiguous_frame_times = True

        # Mock return values for some sync functions
        photodiode_times = np.array([0, 1, 2, 3, 4])
        vsync_times = np.array([0.5, 1.5, 2.5, 3.5])

        vsync_times_chunked = [vsync_times[:2], vsync_times[2:]]
        pd_times_chunked = [photodiode_times[:3], photodiode_times[3:]]

        frame_starts_chunk_1 = np.array([0.1, 0.2])
        frame_starts_chunk_2 = np.array([0.4, 0.5])

        final_frame_start_times = np.concatenate(
            (frame_starts_chunk_1, frame_starts_chunk_2)
        )

        with patch(
            "aind_metadata_mapper.open_ephys.utils"
            ".stim_utils.sync.get_edges",
            side_effect=[photodiode_times, vsync_times],
        ):
            with patch(
                "aind_metadata_mapper.open_ephys.utils"
                ".stim_utils.sync.separate_vsyncs_and_photodiode_times",
                return_value=(vsync_times_chunked, pd_times_chunked),
            ):
                with patch(
                    "aind_metadata_mapper.open_ephys.utils"
                    ".stim_utils.sync.compute_frame_times",
                    side_effect=[
                        (None, frame_starts_chunk_1, None),
                        (None, frame_starts_chunk_2, None),
                    ],
                ):
                    with patch(
                        "aind_metadata_mapper.open_ephys.utils"
                        ".stim_utils.sync.remove_zero_frames",
                        return_value=final_frame_start_times,
                    ):
                        with patch(
                            "aind_metadata_mapper.open_ephys.utils"
                            ".stim_utils.sync.trimmed_stats",
                            return_value=[1.9, 2.2],
                        ):
                            with patch(
                                "aind_metadata_mapper.open_ephys.utils"
                                ".stim_utils.sync.correct_on_off_effects",
                                return_value=[1.9, 2.2],
                            ):
                                result_frame_start_times = (
                                    stim.extract_frame_times_from_photodiode(
                                        sync_file,
                                        photodiode_cycle,
                                        frame_keys,
                                        photodiode_keys,
                                        trim_discontiguous_frame_times,
                                    )
                                )
                                np.testing.assert_array_equal(
                                    result_frame_start_times,
                                    final_frame_start_times,
                                )

    def test_convert_frames_to_seconds(self):
        """
        Tests the convert_frames_to_seconds function.
        """
        # Sample input data
        stimulus_table = pd.DataFrame(
            {
                "start_frame": [0, 10, 20],
                "stop_frame": [5, 15, 25],
                "start_time": [1, 2, 3],
                "stop_time": [0, 1, 2],
            }
        )
        frame_times = np.array(
            [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
        )  # 0.1 second per frame
        frames_per_second = 10
        extra_frame_time = False
        expected_stimulus_table = pd.DataFrame(
            {
                "start_frame": [0, 10, 20],
                "stop_frame": [5, 15, 25],
                "start_time": [0.1, 0.2, 0.3],
                "stop_time": [0.0, 0.1, 0.2],
            }
        )

        # Call the function
        result_stimulus_table = stim.convert_frames_to_seconds(
            stimulus_table, frame_times, frames_per_second, extra_frame_time
        )

        # Check if the modified stimulus table matches the expected one
        pd.testing.assert_frame_equal(
            result_stimulus_table, expected_stimulus_table
        )

    def test_apply_display_sequence(self):
        """
        Tests application of display sequences
        """
        # Sample input data
        sweep_frames_table = pd.DataFrame(
            {"start_time": [0, 5, 10], "stop_time": [3, 8, 18]}
        )
        frame_display_sequence = np.array([[0, 10], [15, 25], [30, 40]])
        expected_sweep_frames_table = pd.DataFrame(
            {
                "start_time": [0, 5, 15],
                "stop_time": [3, 8, 23],
                "stim_block": [0, 0, 1],
            }
        )

        # Call the function
        result_sweep_frames_table = stim.apply_display_sequence(
            sweep_frames_table, frame_display_sequence
        )

        # Check if the modified sweep frames table matches the expected one
        pd.testing.assert_frame_equal(
            result_sweep_frames_table, expected_sweep_frames_table
        )

    def test_get_image_set_name(self):
        """
        Tests the get_image_set_name function.
        """
        # Sample input data
        image_set_path = "/path/to/image_set/image_set_name.jpg"
        expected_image_set_name = "image_set_name"

        # Call the function
        result_image_set_name = stim.get_image_set_name(image_set_path)

        # Check if the result matches the expected image set name
        self.assertEqual(result_image_set_name, expected_image_set_name)

    def test_read_stimulus_name_from_path(self):
        """
        Tests the read_stimulus_name_from_path function.
        """
        # Sample input data
        stimulus = {"stim_path": r"path\to\stimuli\stimulus_name.jpg"}
        expected_stimulus_name = "stimulus_name"

        # Call the function
        result_stimulus_name = stim.read_stimulus_name_from_path(stimulus)

        # Check if the result matches the expected stimulus name
        self.assertEqual(result_stimulus_name, expected_stimulus_name)

    def test_get_stimulus_type(self):
        """
        Tests the get_stimulus_type function.
        """
        # Sample input data
        stimulus = {"stim": "name='image_stimulus'"}
        expected_stimulus_type = "image_stimulus"

        # Call the function
        result_stimulus_type = stim.get_stimulus_type(stimulus)

        # Check if the result matches the expected stimulus type
        self.assertEqual(result_stimulus_type, expected_stimulus_type)

    def setUp(self):
        """
        Sets up a fake stim
        """
        self.stimulus = {
            "display_sequence": [0, 10],
            "sweep_frames": [[0, 5], [7, 12]],
            "sweep_order": [0, 1],
            "stim": "name='image_stimulus'",
            "dimnames": ["Contrast", "Orientation"],
            "sweep_table": [[0.5, 45], [0.7, 90]],
        }

    @patch(
        "aind_metadata_mapper.open_ephys.utils.stim_utils.seconds_to_frames"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".stim_utils.read_stimulus_name_from_path"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils" ".stim_utils.get_stimulus_type"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".stim_utils.apply_display_sequence"
    )
    @patch(
        "aind_metadata_mapper.open_ephys.utils"
        ".stim_utils.assign_sweep_values"
    )
    @patch("aind_metadata_mapper.open_ephys.utils.stim_utils.split_column")
    @patch("aind_metadata_mapper.open_ephys.utils.stim_utils.parse_stim_repr")
    def test_build_stimuluswise_table(
        self,
        mock_parse_stim_repr,
        mock_split_column,
        mock_assign_sweep_values,
        mock_apply_display_sequence,
        mock_get_stimulus_type,
        mock_read_stimulus_name_from_path,
        mock_seconds_to_frames,
    ):
        """
        Tests building of a stimwise table
        Mocks most imports for the function

        """
        # Mock functions
        mock_seconds_to_frames.return_value = [0, 10]
        mock_read_stimulus_name_from_path.return_value = "image_stimulus"
        mock_get_stimulus_type.return_value = "image_stimulus"
        mock_apply_display_sequence.return_value = pd.DataFrame(
            {"start_time": [0, 5], "stop_time": [5, 10], "stim_block": [0, 0]}
        )
        mock_parse_stim_repr.return_value = {
            "Contrast": 0.5,
            "Orientation": 45,
        }
        mock_split_column.return_value = pd.DataFrame(
            {
                "start_time": [0, 5],
                "stop_time": [5, 10],
                "stim_block": [0, 0],
                "Contrast": [0.5, 0.7],
                "Orientation": [45, 90],
            }
        )
        mock_assign_sweep_values.return_value = pd.DataFrame(
            {
                "start_time": [0, 5],
                "stop_time": [5, 10],
                "stim_block": [0, 0],
                "Contrast": [0.5, 0.7],
                "Orientation": [45, 90],
            }
        )

        # Call the function
        result = stim.build_stimuluswise_table(
            None, self.stimulus, MagicMock()
        )

        # Assert the result
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], pd.DataFrame)
        self.assertEqual(
            result[0].shape[0], 2
        )  # Assuming 2 sweeps in the test data

    def test_split_column(self):
        """
        Tests splitting of columns
        """
        # Sample input data
        data = {
            "column_to_split": [1, 2, 3, 4],
            "other_column": ["a", "b", "c", "d"],
        }
        df = pd.DataFrame(data)

        # Define new columns and splitting rules
        new_columns = {
            "new_column_1": lambda x: x * 2,
            "new_column_2": lambda x: x + 1,
        }

        # Call the function
        result = stim.split_column(df, "column_to_split", new_columns)

        # Expected result
        expected_data = {
            "other_column": ["a", "b", "c", "d"],
            "new_column_1": [2, 4, 6, 8],
            "new_column_2": [2, 3, 4, 5],
        }
        expected_df = pd.DataFrame(expected_data)

        # Check if the result matches the expected DataFrame
        pd.testing.assert_frame_equal(result, expected_df)

    def test_assign_sweep_values(self):
        """
        Tests the assigning of sweep values
        """
        # Sample input data for stim_table
        stim_data = {
            "start_time": [0, 10, 20],
            "end_time": [5, 15, 25],
            "sweep_number": [0, 1, 2],
        }
        stim_df = pd.DataFrame(stim_data)

        # Sample input data for sweep_table
        sweep_data = {
            "sweep_number": [0, 1, 2],
            "param_1": ["a", "b", "c"],
            "param_2": [1, 2, 3],
        }
        sweep_df = pd.DataFrame(sweep_data)

        # Call the function
        result = stim.assign_sweep_values(stim_df, sweep_df, on="sweep_number")

        # Expected result
        expected_data = {
            "start_time": [0, 10, 20],
            "end_time": [5, 15, 25],
            "param_1": ["a", "b", "c"],
            "param_2": [1, 2, 3],
        }
        expected_df = pd.DataFrame(expected_data)

        # Check if the result matches the expected DataFrame
        pd.testing.assert_frame_equal(result, expected_df)
