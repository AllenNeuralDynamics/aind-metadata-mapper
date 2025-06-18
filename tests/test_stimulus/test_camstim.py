"""Test the camstim.py module"""

import unittest
from datetime import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from aind_data_schema.base import AindGeneric

from aind_metadata_mapper.stimulus.camstim import Camstim, CamstimSettings


class TestCamstim(unittest.TestCase):
    """Test camstim.py"""

    @classmethod
    @patch("pathlib.Path.rglob")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim._get_sync_times")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.get_session_uuid")
    @patch("aind_metadata_mapper.stimulus.camstim.Camstim._is_behavior")
    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.load_pkl")
    @patch("aind_metadata_mapper.open_ephys.utils.sync_utils.load_sync")
    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.get_fps")
    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.get_stage")
    def setUpClass(
        cls,
        mock_get_stage: MagicMock,
        mock_get_fps: MagicMock,
        mock_load_sync: MagicMock,
        mock_load_pkl: MagicMock,
        mock_is_behavior: MagicMock,
        mock_session_uuid: MagicMock,
        mock_sync_times: MagicMock,
        mock_rglob: MagicMock,
    ) -> None:
        """Set up the test suite"""
        mock_get_fps.return_value = 30.0
        mock_load_sync.return_value = {}
        mock_load_pkl.return_value = {
            "fps": 30.0,
            "items": {
                "behavior": {
                    "params": {
                        "stage": "stage",
                    }
                }
            },
        }
        mock_is_behavior.return_value = True
        mock_session_uuid.return_value = "1234"
        mock_sync_times.return_value = (
            dt(2024, 11, 1, 15, 41, 32, 920082),
            dt(2024, 11, 1, 15, 41, 50, 648629),
        )
        mock_get_stage.return_value = "stage"
        mock_rglob.return_value = iter([Path("some/path/file.pkl")])
        cls.camstim = Camstim(
            CamstimSettings(
                input_source="some/path",
                output_directory="some/other/path",
                session_id="1234567890",
                subject_id="123456",
            )
        )
        cls.camstim_settings = CamstimSettings(
            input_source="some/path",
            output_directory="some/other/path",
            session_id="1234567890",
            subject_id="123456",
        )

    @patch(
        "aind_metadata_mapper.stimulus.camstim.sync.get_ophys_stimulus_timestamps"  # noqa
    )
    @patch(
        "aind_metadata_mapper.stimulus.camstim.behavior_utils.from_stimulus_file"  # noqa
    )
    @patch("pandas.DataFrame.to_csv")
    def test_build_behavior_table(
        self,
        mock_to_csv: MagicMock,
        mock_from_stimulus_file: MagicMock,
        mock_get_ophys_stimulus_timestamps: MagicMock,
    ):
        """Test the build_behavior_table method"""
        # Mock the return values
        mock_get_ophys_stimulus_timestamps.return_value = [1, 2, 3]
        mock_from_stimulus_file.return_value = [pd.DataFrame({"a": [1, 2, 3]})]

        # Call the method
        self.camstim.build_behavior_table()

        # Assert the calls
        mock_get_ophys_stimulus_timestamps.assert_called_once_with(
            self.camstim.sync_data, self.camstim.pkl_path
        )
        mock_from_stimulus_file.assert_called_once_with(
            self.camstim.pkl_path, [1, 2, 3]
        )
        mock_to_csv.assert_called_once_with(
            self.camstim.stim_table_path, index=False
        )

    @patch(
        "aind_metadata_mapper.stimulus.camstim.stim_utils.extract_frame_times_from_photodiode"  # noqa
    )
    @patch(
        "aind_metadata_mapper.stimulus.camstim.stim_utils.create_stim_table"
    )
    @patch("aind_metadata_mapper.stimulus.camstim.names.map_column_names")
    @patch("pandas.DataFrame.to_csv")
    @patch(
        "aind_metadata_mapper.stimulus.camstim.stim_utils.seconds_to_frames"
    )
    @patch("aind_metadata_mapper.open_ephys.utils.pkl_utils.get_stimuli")
    @patch(
        "aind_metadata_mapper.open_ephys.utils.stim_utils.extract_blocks_from_stim"  # noqa
    )
    @patch(
        "aind_metadata_mapper.stimulus.camstim.Camstim.get_stim_table_seconds"
    )
    def test_build_stimulus_table(
        self,
        mock_get_stim_table_seconds: MagicMock,
        mock_extract_blocks_from_stim: MagicMock,
        mock_get_stimuli: MagicMock,
        mock_seconds_to_frames: MagicMock,
        mock_to_csv: MagicMock,
        mock_map_column_names: MagicMock,
        mock_create_stim_table: MagicMock,
        mock_extract_frame_times_from_photodiode: MagicMock,
    ):
        """Test the build_stimulus_table method"""
        # Mock the return values
        mock_get_stim_table_seconds.return_value = [
            pd.DataFrame({"a": [1, 2, 3]})
        ]
        mock_extract_blocks_from_stim.return_value = [1, 2, 3]
        mock_get_stimuli.return_value = {"stuff": "things"}
        mock_seconds_to_frames.return_value = np.array([1, 2, 3])
        mock_extract_frame_times_from_photodiode.return_value = [0.1, 0.2, 0.3]
        mock_create_stim_table.return_value = pd.DataFrame({"a": [1, 2, 3]})
        mock_map_column_names.return_value = pd.DataFrame({"a": [1, 2, 3]})

        # Call the method
        self.camstim.build_stimulus_table()

        # Assert the calls
        mock_extract_frame_times_from_photodiode.assert_called_once()
        mock_create_stim_table.assert_called_once()
        mock_map_column_names.assert_called_once()
        mock_to_csv.assert_called_once_with(
            self.camstim.stim_table_path, index=False
        )

    def test_extract_stim_epochs(self):
        """Test the extract_stim_epochs method"""
        # Create a mock stimulus table
        data = {
            "start_time": [0, 1, 2, 3, 4],
            "stop_time": [1, 2, 3, 4, 5],
            "stim_name": ["stim1", "stim1", "stim2", "stim2", "stim3"],
            "stim_type": ["type1", "type1", "type2", "type2", "type3"],
            "frame": [0, 1, 2, 3, 4],
            "param1": ["a", "a", "b", "b", "c"],
            "param2": [1, 1, 2, 2, 3],
        }
        stim_table = pd.DataFrame(data)

        # Expected output
        expected_epochs = [
            ["stim1", 0, 2, {"param1": {"a"}, "param2": {1}}, set()],
            ["stim2", 2, 4, {"param1": {"b"}, "param2": {2}}, set()],
            # ["stim3", 4, 5, {"param1": {"c"}, "param2": {3}}, set()],
        ]

        # Call the method
        epochs = self.camstim.extract_stim_epochs(stim_table)

        # Assert the result
        self.assertEqual(epochs, expected_epochs)

    def test_extract_stim_epochs_with_images_and_movies(self):
        """Test the extract_stim_epochs method with images and movies"""
        # Create a mock stimulus table with images and movies
        data = {
            "start_time": [0, 1, 2, 3, 4],
            "stop_time": [1, 2, 3, 4, 5],
            "stim_name": ["image1", "image1", "movie1", "movie1", "stim3"],
            "stim_type": ["type1", "type1", "type2", "type2", "type3"],
            "frame": [0, 1, 2, 3, 4],
            "param1": ["a", "a", "b", "b", "c"],
            "param2": [1, 1, 2, 2, 3],
        }
        stim_table = pd.DataFrame(data)

        # Expected output
        # expected_epochs = [
        #     ["image1", 0, 2, {"param1": {"a"}, "param2": {1}}, {"image1"}],
        #     ["movie1", 2, 4, {"param1": {"b"}, "param2": {2}}, {"movie1"}],
        #     ["stim3", 4, 5, {"param1": {"c"}, "param2": {3}}, set()],
        # ]
        expected_epochs = [
            ["image1", 0, 2, {"param1": {"a"}, "param2": {1}}, {"image1"}],
            ["movie1", 2, 4, {"param1": {"b"}, "param2": {2}}, {"movie1"}],
        ]
        # Call the method
        epochs = self.camstim.extract_stim_epochs(stim_table)
        # Assert the result
        self.assertEqual(epochs, expected_epochs)

    @patch("aind_metadata_mapper.stimulus.camstim.Camstim.extract_stim_epochs")
    @patch("pandas.read_csv")
    def test_epochs_from_stim_table(
        self, mock_read_csv: MagicMock, mock_extract_stim_epochs: MagicMock
    ):
        """Test the epochs_from_stim_table method"""
        # Mock the return values
        mock_read_csv.return_value = pd.DataFrame(
            {
                "start_time": [0, 1, 2],
                "stop_time": [1, 2, 3],
                "stim_name": ["stim1", "stim2", "stim3"],
                "stim_type": ["type1", "type2", "type3"],
                "frame": [0, 1, 2],
                "param1": ["a", "b", "c"],
                "param2": [1, 2, 3],
            }
        )
        mock_extract_stim_epochs.return_value = [
            ["stim1", 0, 1, {"param1": {"a"}, "param2": {1}}, set()],
            ["stim2", 1, 2, {"param1": {"b"}, "param2": {2}}, set()],
            ["stim3", 2, 3, {"param1": {"c"}, "param2": {3}}, set()],
        ]

        # Call the method
        schema_epochs = self.camstim.epochs_from_stim_table()

        # Assert the result
        self.assertEqual(len(schema_epochs), 3)
        self.assertEqual(schema_epochs[0].stimulus_name, "stim1")
        self.assertEqual(schema_epochs[1].stimulus_name, "stim2")
        self.assertEqual(schema_epochs[2].stimulus_name, "stim3")
        self.assertEqual(
            schema_epochs[0].stimulus_parameters[0].stimulus_parameters,
            AindGeneric(param1={"a"}, param2={1}),
        )
        self.assertEqual(
            schema_epochs[1].stimulus_parameters[0].stimulus_parameters,
            AindGeneric(param1={"b"}, param2={2}),
        )
        self.assertEqual(
            schema_epochs[2].stimulus_parameters[0].stimulus_parameters,
            AindGeneric(param1={"c"}, param2={3}),
        )

    @patch(
        "aind_metadata_mapper.stimulus.camstim.stim_utils.convert_frames_to_seconds"  # noqa
    )
    @patch("aind_metadata_mapper.stimulus.camstim.names.collapse_columns")
    @patch("aind_metadata_mapper.stimulus.camstim.names.drop_empty_columns")
    @patch(
        "aind_metadata_mapper.stimulus.camstim.names.standardize_movie_numbers"
    )
    @patch(
        "aind_metadata_mapper.stimulus.camstim.names.add_number_to_shuffled_movie"  # noqa
    )
    @patch("aind_metadata_mapper.stimulus.camstim.names.map_stimulus_names")
    def test_get_stim_table_seconds(
        self,
        mock_map_stimulus_names: MagicMock,
        mock_add_number_to_shuffled_movie: MagicMock,
        mock_standardize_movie_numbers: MagicMock,
        mock_drop_empty_columns: MagicMock,
        mock_collapse_columns: MagicMock,
        mock_convert_frames_to_seconds: MagicMock,
    ):
        """Test the get_stim_table_seconds method"""
        # Mock the return values
        mock_convert_frames_to_seconds.return_value = pd.DataFrame(
            {"a": [1, 2, 3]}
        )
        mock_collapse_columns.return_value = pd.DataFrame({"a": [1, 2, 3]})
        mock_drop_empty_columns.return_value = pd.DataFrame({"a": [1, 2, 3]})
        mock_standardize_movie_numbers.return_value = pd.DataFrame(
            {"a": [1, 2, 3]}
        )
        mock_add_number_to_shuffled_movie.return_value = pd.DataFrame(
            {"a": [1, 2, 3]}
        )
        mock_map_stimulus_names.return_value = pd.DataFrame({"a": [1, 2, 3]})

        # Call the method
        stim_table_sweeps = pd.DataFrame({"frame": [1, 2, 3]})
        frame_times = [0.1, 0.2, 0.3]
        name_map = {"old_name": "new_name"}

        result = self.camstim.get_stim_table_seconds(
            stim_table_sweeps, frame_times, name_map
        )
        # Assert the calls
        mock_convert_frames_to_seconds.assert_called_once_with(
            stim_table_sweeps, frame_times, 30.0, True
        )
        mock_collapse_columns.assert_called_once()
        mock_drop_empty_columns.assert_called_once()
        mock_standardize_movie_numbers.assert_called_once()
        mock_add_number_to_shuffled_movie.assert_called_once()
        mock_map_stimulus_names.assert_called_once()

        # Assert the result
        expected_result = pd.DataFrame({"a": [1, 2, 3]})
        pd.testing.assert_frame_equal(result, expected_result)


if __name__ == "__main__":
    unittest.main()
