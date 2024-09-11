""" Utilities for working with stimulus data."""

import ast
import functools
import logging
import re
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

import aind_metadata_mapper.open_ephys.utils.pkl_utils as pkl
import aind_metadata_mapper.open_ephys.utils.sync_utils as sync

logger = logging.getLogger(__name__)

DROP_PARAMS = (  # psychopy boilerplate, more or less
    "autoLog",
    "autoDraw",
    "win",
)

REPR_PARAMS_RE = re.compile(r"([a-z0-9]+=[^=]+)[,\)]", re.IGNORECASE)
REPR_CLASS_RE = re.compile(r"^(?P<class_name>[a-z0-9]+)\(.*\)$", re.IGNORECASE)
ARRAY_RE = re.compile(r"array\((?P<contents>\[.*\])\)")

FRAME_KEYS = ("frames", "stim_vsync", "vsync_stim")
PHOTODIODE_KEYS = ("photodiode", "stim_photodiode")
OPTOGENETIC_STIMULATION_KEYS = ("LED_sync", "opto_trial")
EYE_TRACKING_KEYS = (
    "eye_frame_received",  # Expected eye tracking
    # line label after 3/27/2020
    # clocks eye tracking frame pulses (port 0, line 9)
    "cam2_exposure",
    # previous line label for eye tracking
    # (prior to ~ Oct. 2018)
    "eyetracking",
    "eye_cam_exposing",
    "eye_tracking",
)  # An undocumented, but possible eye tracking line label  # NOQA E114
BEHAVIOR_TRACKING_KEYS = (
    "beh_frame_received",  # Expected behavior line label after 3/27/2020  # NOQA E127
    # clocks behavior tracking frame # NOQA E127
    # pulses (port 0, line 8)
    "cam1_exposure",
    "behavior_monitoring",
)


def convert_filepath_caseinsensitive(filename_in):
    """
    Replaces the case of training

    Parameters
    ----------
    filename_in : str
        The filename to convert

    Returns
    -------
    str
        The filename with the case replaced
    """
    return filename_in.replace("TRAINING", "training")


def enforce_df_int_typing(
    input_df: pd.DataFrame,
    int_columns: List[str],
    use_pandas_type: object = False,
) -> pd.DataFrame:
    """Enforce integer typing for columns that may have lost int typing when
    combined into the final DataFrame.

    Parameters
    ----------
    input_df : pandas.DataFrame
        DataFrame with typing to enforce.
    int_columns : list of str
        Columns to enforce int typing and fill any NaN/None values with the
        value set in INT_NULL in this file. Requested columns not in the
        dataframe are ignored.
    use_pandas_type : bool
        Instead of filling with the value INT_NULL to enforce integer typing,
        use the pandas type Int64. This type can have issues converting to
        numpy/array type values.

    Returns
    -------
    output_df : pandas.DataFrame
        DataFrame specific columns hard typed to Int64 to allow NA values
        without resorting to float type.
    """
    for col in int_columns:
        if col in input_df.columns:
            if use_pandas_type:
                input_df[col] = input_df[col].astype("Int64")
            else:
                input_df[col] = input_df[col].fillna().astype(int)
    return input_df


def enforce_df_column_order(
    input_df: pd.DataFrame, column_order: List[str]
) -> pd.DataFrame:
    """Return the data frame but with columns ordered.

    Parameters
    ----------
    input_df : pandas.DataFrame
        Data frame with columns to be ordered.
    column_order : list of str
        Ordering of column names to enforce. Columns not specified are shifted
        to the end of the order but retain their order amongst others not
        specified. If a specified column is not in the DataFrame it is ignored.

    Returns
    -------
    output_df : pandas.DataFrame
        DataFrame the same as the input but with columns reordered.
    """
    # Use only columns that are in the input dataframe's columns.
    pruned_order = []
    for col in column_order:
        if col in input_df.columns:
            pruned_order.append(col)
    # Get the full list of columns in the data frame with our ordered columns
    # first.
    pruned_order.extend(
        list(set(input_df.columns).difference(set(pruned_order)))
    )
    return input_df[pruned_order]


def seconds_to_frames(seconds, pkl_file):
    """
    Convert seconds to frames using the pkl file.

    Parameters
    ----------
    seconds : list of float
        Seconds to convert to frames.
    pkl_file : str
        Path to the pkl file.

    Returns
    -------
    frames : list of int
        Frames corresponding to the input seconds.
    """
    return (np.array(seconds) + pkl.get_pre_blank_sec(pkl_file)) * pkl.get_fps(
        pkl_file
    )


def extract_const_params_from_stim_repr(
    stim_repr, repr_params_re=REPR_PARAMS_RE, array_re=ARRAY_RE
):
    """Parameters which are not set as sweep_params in the stimulus script
    (usually because they are not varied during the course of the session) are
    not output in an easily machine-readable format. This function
    attempts to recover them by parsing the string repr of the stimulus.

    Parameters
    ----------
        stim_repr : str
            The repr of the camstim stimulus object. Served up per-stimulus
            in the stim pickle.
        repr_params_re : re.Pattern
            Extracts attributes as "="-seperated strings
        array_re : re.Pattern
            Extracts list reprs from numpy array reprs.

    Returns
    -------
    repr_params : dict
        dictionary of paramater keys and values extracted from the stim repr.
        Where possible, the values are converted to native Python types.

    """

    repr_params = {}

    for match in repr_params_re.findall(stim_repr):
        k, v = match.split("=")

        if k not in repr_params:
            m = array_re.match(v)
            if m is not None:
                v = m["contents"]

            try:
                v = ast.literal_eval(v)
            except ValueError:
                pass

            repr_params[k] = v

        else:
            raise KeyError(f"duplicate key: {k}")

    return repr_params


def parse_stim_repr(
    stim_repr,
    drop_params=DROP_PARAMS,
    repr_params_re=REPR_PARAMS_RE,
    array_re=ARRAY_RE,
    raise_on_unrecognized=False,
):
    """Read the string representation of a psychopy stimulus and extract
    stimulus parameters.

    Parameters
    ----------
    stim_repr : str
    drop_params : tuple
    repr_params_re : re.Pattern
    array_re : re.Pattern


    Returns
    -------
    dict :
        maps extracted parameter names to values

    """

    stim_params = extract_const_params_from_stim_repr(
        stim_repr, repr_params_re=repr_params_re, array_re=array_re
    )

    for drop_param in drop_params:
        if drop_param in stim_params:
            del stim_params[drop_param]

    logger.debug(stim_params)
    return stim_params


def create_stim_table(
    pkl_file,
    stimuli,
    stimulus_tabler,
    spontaneous_activity_tabler,
    sort_key="start_time",
    block_key="stim_block",
    index_key="stim_index",
):
    """Build a full stimulus table

    Parameters
    ----------
    stimuli : list of dict
        Each element is a stimulus dictionary,
        as provided by the stim.pkl file.
    stimulus_tabler : function
        A function which takes a single stimulus dictionary
        as its argument and returns a stimulus table dataframe.
    spontaneous_activity_tabler : function
        A function which takes a list of stimulus tables as
        arguments and returns a list of 0 or more tables
        describing spontaneous activity sweeps.
    sort_key : str, optional
        Sort the final stimulus table in ascending order by this key.
        Defaults to 'start_time'.

    Returns
    -------
    stim_table_full : pandas.DataFrame
        Each row is a sweep. Has columns describing (in frames) the start
        and end times of each sweep. Other columns
        describe the values of stimulus parameters on those sweeps.

    """

    stimulus_tables = []
    for ii, stimulus in enumerate(stimuli):
        current_tables = stimulus_tabler(pkl_file, stimulus)
        for table in current_tables:
            table[index_key] = ii

        stimulus_tables.extend(current_tables)

    stimulus_tables = sorted(
        stimulus_tables, key=lambda df: min(df[sort_key].values)
    )
    for ii, stim_table in enumerate(stimulus_tables):
        stim_table[block_key] = ii

    stimulus_tables.extend(spontaneous_activity_tabler(stimulus_tables))

    stim_table_full = pd.concat(stimulus_tables, ignore_index=True, sort=False)
    stim_table_full.sort_values(by=[sort_key], inplace=True)
    stim_table_full.reset_index(drop=True, inplace=True)

    return stim_table_full


def make_spontaneous_activity_tables(
    stimulus_tables,
    start_key="start_time",
    end_key="stop_time",
    duration_threshold=0.0,
):
    """Fills in frame gaps in a set of stimulus tables. Suitable for use as
    the spontaneous_activity_tabler in create_stim_table.

    Parameters
    ----------
    stimulus_tables : list of pd.DataFrame
        Input tables - should have start_key and end_key columns.
    start_key : str, optional
        Column name for the start of a sweep. Defaults to 'start_time'.
    end_key : str, optional
        Column name for the end of a sweep. Defaults to 'stop_time'.
    duration_threshold : numeric or None
        If not None (default is 0), remove spontaneous activity sweeps
        whose duration is less than this threshold.

    Returns
    -------
    list :
        Either empty, or contains a single pd.DataFrame.
        The rows of the dataframe are spontaneous activity sweeps.

    """

    nstimuli = len(stimulus_tables)
    if nstimuli == 0:
        return []

    spon_start = np.zeros(nstimuli + 1, dtype=int)
    spon_end = np.zeros(nstimuli, dtype=int)

    for ii, table in enumerate(stimulus_tables):
        spon_start[ii + 1] = table[end_key].values[-1]
        spon_end[ii] = table[start_key].values[0]

    spon_start = spon_start[:-1]
    spon_sweeps = pd.DataFrame({start_key: spon_start, end_key: spon_end})

    if duration_threshold is not None:
        spon_sweeps = spon_sweeps[
            np.fabs(spon_sweeps[start_key] - spon_sweeps[end_key])
            > duration_threshold
        ]
        spon_sweeps.reset_index(drop=True, inplace=True)

    return [spon_sweeps]


def extract_blocks_from_stim(stims):
    """
    Extracts the blocks from a stim with multiple blocks

    Parameters
    ----------
    stims: List[dict]
        A list of stimuli dictionaries

    Returns
    -------
    stim_chunked_blocks: List[dict]
        A list of stimuli dictionaries with the blocks extracted
    """
    stim_chunked_blocks = []
    for stimulus in stims:
        if 'stimuli' in stimulus:
            for stimulus_block in stimulus['stimuli']:
                stim_chunked_blocks.append(stimulus_block)
        else:
            stim_chunked_blocks.append(stimulus)
    return stim_chunked_blocks


def extract_frame_times_from_photodiode(
    sync_file,
    photodiode_cycle=60,
    frame_keys=FRAME_KEYS,
    photodiode_keys=PHOTODIODE_KEYS,
    trim_discontiguous_frame_times=True,
):
    """
    Extracts frame times from a photodiode signal.

    Parameters
    ----------
    sync_file : h5py.File
        File containing sync data.
    photodiode_cycle : numeric, optional
        The number of frames between photodiode pulses. Defaults to 60.
    frame_keys : tuple of str, optional
        Keys to extract frame times from. Defaults to FRAME_KEYS.
    photodiode_keys : tuple of str, optional
        Keys to extract photodiode times from. Defaults to PHOTODIODE_KEYS.
    trim_discontiguous_frame_times : bool, optional
        If True, remove discontiguous frame times. Defaults to True.

    Returns
    -------
    frame_start_times : np.ndarray
        The start times of each frame.

    """

    photodiode_times = sync.get_edges(sync_file, "all", photodiode_keys)
    vsync_times = sync.get_edges(sync_file, "falling", frame_keys)

    if trim_discontiguous_frame_times:
        vsync_times = sync.trim_discontiguous_vsyncs(vsync_times)

    (
        vsync_times_chunked,
        pd_times_chunked,
    ) = sync.separate_vsyncs_and_photodiode_times(
        vsync_times, photodiode_times, photodiode_cycle
    )

    frame_start_times = np.zeros((0,))

    for i in range(len(vsync_times_chunked)):
        photodiode_times = sync.trim_border_pulses(
            pd_times_chunked[i], vsync_times_chunked[i]
        )
        photodiode_times = sync.correct_on_off_effects(photodiode_times)
        photodiode_times = sync.fix_unexpected_edges(
            photodiode_times, cycle=photodiode_cycle
        )

        frame_duration = sync.estimate_frame_duration(
            photodiode_times, cycle=photodiode_cycle
        )
        irregular_interval_policy = functools.partial(
            sync.allocate_by_vsync, np.diff(vsync_times_chunked[i])
        )
        (
            frame_indices,
            frame_starts,
            frame_end_times,
        ) = sync.compute_frame_times(
            photodiode_times,
            frame_duration,
            len(vsync_times_chunked[i]),
            cycle=photodiode_cycle,
            irregular_interval_policy=irregular_interval_policy,
        )

        frame_start_times = np.concatenate((frame_start_times, frame_starts))

    frame_start_times = sync.remove_zero_frames(frame_start_times)

    return frame_start_times


def convert_frames_to_seconds(
    stimulus_table,
    frame_times,
    frames_per_second=None,
    extra_frame_time=False,
    map_columns=("start_time", "stop_time"),
):
    """Converts sweep times from frames to seconds.

    Parameters
    ----------
    stimulus_table : pd.DataFrame
        Rows are sweeps. Columns are stimulus parameters as well as start
        and end frames for each sweep.
    frame_times : numpy.ndarrray
        Gives the time in seconds at which each frame (indices) began.
    frames_per_second : numeric, optional
        If provided, and extra_frame_time is True, will be used to calculcate
        the extra_frame_time.
    extra_frame_time : float, optional
        If provided, an additional frame time will be appended. The time will
        be incremented by extra_frame_time from
        the previous last frame time, to denote the time at which the last
        frame ended. If False, no extra time will be
        appended. If None (default), the increment will be 1.0/fps.
    map_columns : tuple of str, optional
        Which columns to replace with times. Defaults to 'start_time'
        and 'stop_time'

    Returns
    -------
    stimulus_table : pd.DataFrame
        As above, but with map_columns values converted to seconds from frames.

    """

    stimulus_table = stimulus_table.copy()

    if extra_frame_time is True and frames_per_second is not None:
        extra_frame_time = 1.0 / frames_per_second
    if extra_frame_time is not False:
        frame_times = np.append(
            frame_times, frame_times[-1] + extra_frame_time
        )

    for column in map_columns:
        stimulus_table[column] = frame_times[
            np.around(stimulus_table[column]).astype(int)
        ]

    return stimulus_table


def apply_display_sequence(
    sweep_frames_table,
    frame_display_sequence,
    start_key="start_time",
    end_key="stop_time",
    diff_key="dif",
    block_key="stim_block",
):
    """Adjust raw sweep frames for a stimulus based on the display sequence
    for that stimulus.

    Parameters
    ----------
    sweep_frames_table : pd.DataFrame
        Each row is a sweep. Has two columns, 'start' and 'end',
        which describe (in frames) when that sweep began and ended.
    frame_display_sequence : np.ndarray
        2D array. Rows are display intervals. The 0th column is the start
        frame of that interval, the 1st the end frame.

    Returns
    -------
    sweep_frames_table : pd.DataFrame
        As above, but start and end frames have been adjusted based on
        the display sequence.

    Notes
    -----
    The frame values in the raw sweep_frames_table are given in 0-indexed
    offsets from the start of display for this stimulus. This domain only
    takes into account frames which are part of a display interval for that
    stimulus, so the frame ids need to be adjusted to lie on the global
    frame sequence.

    """

    sweep_frames_table = sweep_frames_table.copy()
    if block_key not in sweep_frames_table.columns.values:
        sweep_frames_table[block_key] = np.zeros(
            (sweep_frames_table.shape[0]), dtype=int
        )

    sweep_frames_table[diff_key] = (
        sweep_frames_table[end_key] - sweep_frames_table[start_key]
    )

    sweep_frames_table[start_key] += frame_display_sequence[0, 0]
    for seg in range(len(frame_display_sequence) - 1):
        match_inds = (
            sweep_frames_table[start_key] >= frame_display_sequence[seg, 1]
        )

        sweep_frames_table.loc[match_inds, start_key] += (
            frame_display_sequence[seg + 1, 0] - frame_display_sequence[seg, 1]
        )
        sweep_frames_table.loc[match_inds, block_key] = seg + 1

    sweep_frames_table[end_key] = (
        sweep_frames_table[start_key] + sweep_frames_table[diff_key]
    )
    sweep_frames_table = sweep_frames_table[
        sweep_frames_table[end_key] <= frame_display_sequence[-1, 1]
    ]
    sweep_frames_table = sweep_frames_table[
        sweep_frames_table[start_key] <= frame_display_sequence[-1, 1]
    ]

    sweep_frames_table.drop(diff_key, inplace=True, axis=1)
    return sweep_frames_table


def get_image_set_name(image_set_path: str):
    """
    Strips the stem from the image_set filename
    """
    return Path(image_set_path).stem


def read_stimulus_name_from_path(stimulus):
    """Obtains a human-readable stimulus name by looking at the filename of
    the 'stim_path' item.

    Parameters
    ----------
    stimulus : dict
        must contain a 'stim_path' item.

    Returns
    -------
    str :
        name of stimulus

    """

    stim_name = stimulus["stim_path"].split("\\")[-1].split(".")[0]

    if stimulus["stim_path"] == "":
        stim_name = stimulus["movie_local_path"].split("\\")[-1].split(".")[0]
    else:
        stim_name = stimulus["stim_path"].split("\\")[-1].split(".")[0]
    return stim_name


def get_stimulus_type(stimulus):
    """
    Obtains the stimulus type from the stimulus dictionary.

    Parameters
    ----------
    stimulus : dict
        A dictionary describing a stimulus.

    Returns
    -------
    str :
        The stimulus type.
    """
    input_string = stimulus["stim"]

    # Regex for single quotes
    pattern = r"name='([^']+)'"

    match = re.search(pattern, input_string)

    if match:
        stim_type = match.group(1)
        stim_type = stim_type.replace("unnamed ", "")
        return stim_type
    else:
        return "None or Blank"


def build_stimuluswise_table(
    pickle_file,
    stimulus,
    seconds_to_frames,
    start_key="start_time",
    end_key="stop_time",
    name_key="stim_name",
    template_key="stim_type",
    block_key="stim_block",
    get_stimulus_name=None,
    extract_const_params_from_repr=False,
    drop_const_params=DROP_PARAMS,
):
    """Construct a table of sweeps, including their times on the
    experiment-global clock and the values of each relevant parameter.

    Parameters
    ----------
    stimulus : dict
        Describes presentation of a stimulus on a particular experiment. Has
        a number of fields, of which we are using:
            stim_path : str
                windows file path to the stimulus data
            sweep_frames : list of lists
                rows are sweeps, columns are start and end frames of that sweep
                (in the stimulus-specific frame domain). C-order.
            sweep_order : list of int
                indices are frames, values are the sweep on that frame
            display_sequence : list of list
                rows are intervals in which the stimulus was displayed.
                Columns are start and end times (s, global) of the display.
                C-order.
             dimnames : list of str
                Names of parameters for this stimulus (such as "Contrast")
            sweep_table : list of tuple
                Each element is a tuple of parameter values (1 per dimname)
                describing a single sweep.
    seconds_to_frames : function
        Converts experiment seconds to frames
    start_key : str, optional
        key to use for start frame indices. Defaults to 'start_time'
    end_key : str, optional
        key to use for end frame indices. Defaults to 'stop_time'
    name_key : str, optional
        key to use for stimulus name annotations. Defaults to 'stim_name'
    block_key : str, optional
        key to use for the 0-index position of this stimulus block
    get_stimulus_name : function | dict -> str, optional
        extracts stimulus name from the stimulus dictionary. Default is
        read_stimulus_name_from_path

    Returns
    -------
    list of pandas.DataFrame :
        Each table corresponds to an entry in the display sequence.
        Rows are sweeps, columns are stimulus parameter values as well as
        "start_time" and 'stop_time.

    """

    if get_stimulus_name is None:
        get_stimulus_name = read_stimulus_name_from_path

    frame_display_sequence = seconds_to_frames(
        stimulus["display_sequence"], pickle_file
    )

    sweep_frames_table = pd.DataFrame(
        stimulus["sweep_frames"], columns=(start_key, end_key)
    )
    sweep_frames_table[block_key] = np.zeros(
        [sweep_frames_table.shape[0]], dtype=int
    )
    sweep_frames_table = apply_display_sequence(
        sweep_frames_table, frame_display_sequence, block_key=block_key
    )

    stim_table = pd.DataFrame(
        {
            start_key: sweep_frames_table[start_key],
            end_key: sweep_frames_table[end_key] + 1,
            name_key: get_stimulus_name(stimulus),
            template_key: get_stimulus_type(stimulus),
            block_key: sweep_frames_table[block_key],
        }
    )

    sweep_order = stimulus["sweep_order"][: len(sweep_frames_table)]
    dimnames = stimulus["dimnames"]

    if not dimnames or "ReplaceImage" in dimnames:
        stim_table["Image"] = sweep_order
    else:
        stim_table["sweep_number"] = sweep_order
        sweep_table = pd.DataFrame(stimulus["sweep_table"], columns=dimnames)
        sweep_table["sweep_number"] = sweep_table.index

        stim_table = assign_sweep_values(stim_table, sweep_table)
        stim_table = split_column(
            stim_table,
            "Pos",
            {"Pos_x": lambda field: field[0], "Pos_y": lambda field: field[1]},
        )

    if extract_const_params_from_repr:
        const_params = parse_stim_repr(
            stimulus["stim"], drop_params=drop_const_params
        )
        existing_columns = set(stim_table.columns)
        for const_param_key, const_param_value in const_params.items():
            existing_cap = const_param_key.capitalize() in existing_columns
            existing_upper = const_param_key.upper() in existing_columns
            existing = const_param_key in existing_columns

            if not (existing_cap or existing_upper or existing):
                stim_table[const_param_key] = [
                    const_param_value
                ] * stim_table.shape[0]
            else:
                raise KeyError(f"column {const_param_key} already exists")

    unique_indices = np.unique(stim_table[block_key].values)
    output = [
        stim_table.loc[stim_table[block_key] == ii, :] for ii in unique_indices
    ]

    return output


def split_column(table, column, new_columns, drop_old=True):
    """Divides a dataframe column into multiple columns.

    Parameters
    ----------
    table : pandas.DataFrame
        Columns will be drawn from and assigned to this dataframe. This
        dataframe will NOT be modified inplace.
    column : str
        This column will be split.
    new_columns : dict, mapping strings to functions
        Each key will be the name of a new column, while its value (a function)
        will be used to build the new column's values. The functions should map
        from a single value of the original column to a single value
        of the new column.
    drop_old : bool, optional
        If True, the original column will be dropped from the table.

    Returns
    -------
    table : pd.DataFrame
        The modified table

    """

    if column not in table:
        return table
    table = table.copy()

    for new_column, rule in new_columns.items():
        table[new_column] = table[column].apply(rule)

    if drop_old:
        table.drop(column, inplace=True, axis=1)
    return table


def assign_sweep_values(
    stim_table,
    sweep_table,
    on="sweep_number",
    drop=True,
    tmp_suffix="_stimtable_todrop",
):
    """Left joins a stimulus table to a sweep table in order to associate
        epochs in time with stimulus characteristics.

    Parameters
    ----------
    stim_table : pd.DataFrame
        Each row is a stimulus epoch, with start and end times and a foreign
        key onto a particular sweep.
    sweep_table : pd.DataFrame
        Each row is a sweep. Should have columns in common with the stim_table
        - the resulting table will use values from the sweep_table.
    on : str, optional
        Column on which to join.
    drop : bool, optional
        If True (default), the join column (argument on) will be dropped from
        the output.
    tmp_suffix : str, optional
        Will be used to identify overlapping columns. Should not appear in the
        name of any column in either dataframe.

    """

    joined_table = stim_table.join(sweep_table, on=on, lsuffix=tmp_suffix)
    for dim in joined_table.columns.values:
        if tmp_suffix in dim:
            joined_table.drop(dim, inplace=True, axis=1)

    if drop:
        joined_table.drop(on, inplace=True, axis=1)
    return joined_table
