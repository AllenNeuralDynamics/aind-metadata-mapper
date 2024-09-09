""" Unit tests for the naming_utils module in the utils package. """

import unittest

import numpy as np
import pandas as pd

from aind_metadata_mapper.open_ephys.utils import naming_utils as naming


class TestDropEmptyColumns(unittest.TestCase):
    """
    Tests naming utils
    """

    def test_drop_empty_columns_all_nan(self):
        """
        Test that columns with all NaN values are dropped.
        """
        # Create a DataFrame with some columns all NaN
        data = {
            "A": [1, 2, 3],
            "B": [None, None, None],
            "C": [4, 5, 6],
            "D": [None, None, None],
        }
        df = pd.DataFrame(data)

        # Expected DataFrame after dropping columns B and D
        expected_data = {"A": [1, 2, 3], "C": [4, 5, 6]}
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.drop_empty_columns(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_drop_empty_columns_no_nan(self):
        """
        Test that columns with no NaN values are not dropped.
        """
        # Create a DataFrame with no columns all NaN
        data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
        df = pd.DataFrame(data)

        # Expected DataFrame (unchanged)
        expected_df = df.copy()

        # Call the function and assert the result
        result_df = naming.drop_empty_columns(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_drop_empty_columns_some_nan(self):
        """
        Test that columns with some NaN values are not dropped.
        """
        # Create a DataFrame with some NaN values but not all in any column
        data = {"A": [1, None, 3], "B": [None, 2, 3], "C": [4, 5, 6]}
        df = pd.DataFrame(data)

        # Expected DataFrame (unchanged)
        expected_df = df.copy()

        # Call the function and assert the result
        result_df = naming.drop_empty_columns(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_drop_empty_columns_all_empty(self):
        """
        Test that columns with all NaN values are dropped.
        """
        # Create a DataFrame with all columns containing only NaN values
        data = {
            "A": [None, None, None],
            "B": [None, None, None],
            "C": [None, None, None],
        }
        df = pd.DataFrame(data)

        expected_df = pd.DataFrame(index=[0, 1, 2])
        # Call the function and assert the result
        result_df = naming.drop_empty_columns(df)
        expected_df.columns = result_df.columns
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_collapse_columns_merge(self):
        """
        Test that columns with the same values are merged.
        """
        # Create a DataFrame with columns that can be merged
        data = {
            "A": [1, None, None],
            "b": [None, 2, None],
            "C": [None, None, 3],
        }
        df = pd.DataFrame(data)

        # Expected DataFrame after merging columns
        expected_data = {
            "A": [1, None, None],
            "b": [None, 2, None],
            "C": [None, None, 3],
        }
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.collapse_columns(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_collapse_columns_no_merge(self):
        """
        Test that columns with different values are not merged.
        """
        # Create a DataFrame with columns that cannot be merged
        data = {
            "A": [1, None, None],
            "B": [None, 2, None],
            "C": [None, None, 3],
        }
        df = pd.DataFrame(data)

        # Expected DataFrame (unchanged)
        expected_df = df.copy()

        # Call the function and assert the result
        result_df = naming.collapse_columns(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_collapse_columns_merge_with_overwrite(self):
        """
        Test that columns with overlapping non-NaN values are merged.
        """
        # Create a DataFrame with overlapping non-NaN columns to be merged
        data = {
            "A": [1, None, None],
            "B": [None, 2, None],
            "C": [None, 3, None],
            "a": [None, 4, None],
            "b": [5, None, None],
            "c": [None, None, 6],
        }
        df = pd.DataFrame(data)

        # Expected DataFrame after merging columns with overwritten NaN values
        expected_data = {
            "a": [1, 4, None],
            "b": [5, 2, None],
            "c": [None, 3, 6],
        }
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.collapse_columns(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_add_number_to_shuffled_movie_no_matching_rows(self):
        """
        Test that rows not matching the shuffled movie regex are unchanged.
        """
        # Create a DataFrame with no rows matching the shuffled movie regex
        data = {
            "stim_name": [
                "natural_movie_1",
                "natural_movie_2",
                "natural_movie_3",
            ]
        }
        df = pd.DataFrame(data)

        # Expected DataFrame (unchanged)
        expected_df = df.copy()

        # Call the function and assert the result
        result_df = naming.add_number_to_shuffled_movie(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_add_number_to_shuffled_movie_multiple_movie_numbers(self):
        """
        Test that an error is raised if multiple different
        movie numbers are found.
        """
        # Create a DataFrame with multiple different movie numbers
        data = {
            "stim_name": [
                "natural_movie_1_shuffled",
                "natural_movie_shuffled",
                "natural_movie_2_shuffled",
                "natural_movie_3_shuffled",
            ]
        }
        df = pd.DataFrame(data)

        # Call the function and assert that it raises a ValueError
        with self.assertRaises(ValueError):
            naming.add_number_to_shuffled_movie(df)

    def test_add_number_to_shuffled_movie_single_movie_number(self):
        """
        Test that the movie number is added to the shuffled movie name.
        """
        # Create a DataFrame with a single movie number
        data = {
            "stim_name": [
                "natural_movie_1",
                "natural_movie_1",
                "natural_movie_1",
            ]
        }
        df = pd.DataFrame(data)

        # Expected DataFrame with the stim_name column modified
        expected_data = {
            "stim_name": [
                "natural_movie_1",
                "natural_movie_1",
                "natural_movie_1",
            ]
        }
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.add_number_to_shuffled_movie(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_add_number_to_shuffled_movie_mixed_columns(self):
        """
        Test that only the matching rows are modified in
        a DataFrame with mixed columns.
        """
        # Create a DataFrame with mixed columns
        # including rows with a shuffled movie regex
        data = {
            "stim_name": [
                "natural_movie_1",
                "image1.jpg",
                "natural_movie_2",
                "natural_movie_3",
            ]
        }
        df = pd.DataFrame(data)

        # Expected DataFrame with only the matching rows modified
        expected_data = {
            "stim_name": [
                "natural_movie_1",
                "image1.jpg",
                "natural_movie_2",
                "natural_movie_3",
            ]
        }
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.add_number_to_shuffled_movie(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_map_stimulus_names_no_mapping(self):
        """
        Test that the DataFrame is unchanged if no mapping is provided.
        """
        # Create a DataFrame with no mapping provided
        data = {"stim_name": ["stim1", "stim2", "stim3"]}
        df = pd.DataFrame(data)

        # Expected DataFrame (unchanged)
        expected_df = df.copy()

        # Call the function and assert the result
        result_df = naming.map_stimulus_names(df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_map_stimulus_names_with_mapping(self):
        """
        Test that the stimulus names are changed according to the mapping.
        """
        # Create a DataFrame with a mapping provided
        data = {"stim_name": ["stim1", "stim2", "stim3"]}
        df = pd.DataFrame(data)
        name_map = {"stim1": "new_stim1", "stim3": "new_stim3"}

        # Change name column with mapping
        expected_data = {"stim_name": ["new_stim1", "stim2", "new_stim3"]}
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.map_stimulus_names(df, name_map=name_map)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_map_stimulus_names_with_nan_mapping(self):
        """
        Test that the stimulus names are changed
        according to the mapping including NaN.
        """
        # Create a DataFrame with a mapping provided including NaN
        data = {"stim_name": ["stim1", "stim2", np.nan]}
        df = pd.DataFrame(data)
        name_map = {"stim1": "new_stim1", np.nan: "new_spontaneous"}

        # Change name column with mapping
        expected_data = {"stim_name": ["new_stim1", "stim2", "spontaneous"]}
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.map_stimulus_names(df, name_map=name_map)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_map_stimulus_names_with_column_name(self):
        """
        Test that the stimulus names are changed
        according to the mapping with a custom column name.
        """
        # Create a DataFrame with a custom stim name
        data = {"custom_stimulus_name": ["stim1", "stim2", "stim3"]}
        df = pd.DataFrame(data)
        name_map = {"stim1": "new_stim1", "stim3": "new_stim3"}

        # Expected DataFrame with names modified to the mapping
        expected_data = {
            "custom_stimulus_name": ["new_stim1", "stim2", "new_stim3"]
        }
        expected_df = pd.DataFrame(expected_data)

        # Call the function with the custom column name and assert the result
        result_df = naming.map_stimulus_names(
            df, name_map=name_map, stim_colname="custom_stimulus_name"
        )
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_map_column_names_with_mapping(self):
        """
        Test that the column names are changed according to the mapping.
        """
        # Create a DataFrame with a mapping provided
        data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
        df = pd.DataFrame(data)
        name_map = {"A": "X", "B": "Y", "C": "Z"}

        # Expected DataFrame with names changed to the mapping
        expected_data = {"X": [1, 2, 3], "Y": [4, 5, 6], "Z": [7, 8, 9]}
        expected_df = pd.DataFrame(expected_data)

        # Call the function and assert the result
        result_df = naming.map_column_names(df, name_map=name_map)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_map_column_names_with_ignore_case(self):
        """
        Test that the column names are changed
        according to the mapping with ignore_case=True.
        """
        # Create a DataFrame with a mapping provided and ignore_case=True
        data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
        df = pd.DataFrame(data)
        name_map = {"a": "X", "b": "Y", "C": "Z"}

        # Expected DataFrame names changed
        # Ignoring case
        expected_data = {"X": [1, 2, 3], "Y": [4, 5, 6], "Z": [7, 8, 9]}
        expected_df = pd.DataFrame(expected_data)

        result_df = naming.map_column_names(
            df, name_map=name_map, ignore_case=True
        )
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_map_column_names_with_ignore_case_false(self):
        """
        Test that the column names are not changed
        according to the mapping with ignore_case=False.
        """
        # Create a DataFrame with a mapping provided and ignore_case=False
        data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
        df = pd.DataFrame(data)
        name_map = {"a": "X", "b": "Y", "c": "Z"}

        # Don't change the column names
        expected_df = df.copy()

        # Call the function with ignore_case=False and assert the result
        result_df = naming.map_column_names(
            df, name_map=name_map, ignore_case=False
        )
        pd.testing.assert_frame_equal(result_df, expected_df)


if __name__ == "__main__":
    unittest.main()
