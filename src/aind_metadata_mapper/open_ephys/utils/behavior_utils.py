""" Utils to process behavior info for stimulus"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd

import aind_metadata_mapper.open_ephys.utils.pkl_utils as pkl
import aind_metadata_mapper.open_ephys.utils.stim_utils as stim

INT_NULL = -99

logger = logging.getLogger(__name__)


def get_stimulus_presentations(data, stimulus_timestamps) -> pd.DataFrame:
    """
    This function retrieves the stimulus presentation dataframe and
    renames the columns, adds a stop_time column, and set's index to
    stimulus_presentation_id before sorting and returning the dataframe.
    :param data: stimulus file associated with experiment id
    :param stimulus_timestamps: timestamps indicating when stimuli switched
                                during experiment
    :return: stimulus_table: dataframe containing the stimuli metadata as well
                             as what stimuli was presented
    """
    stimulus_table = get_visual_stimuli_df(data, stimulus_timestamps)
    # workaround to rename columns to harmonize with visual
    # coding and rebase timestamps to sync time
    stimulus_table.insert(
        loc=0, column="flash_number", value=np.arange(0, len(stimulus_table))
    )
    stimulus_table = stimulus_table.rename(
        columns={
            "frame": "start_frame",
            "time": "start_time",
            "flash_number": "stimulus_presentations_id",
        }
    )
    stimulus_table.start_time = [
        stimulus_timestamps[int(start_frame)]
        for start_frame in stimulus_table.start_frame.values
    ]
    end_time = []
    logger.debug(f"stimulus_table {stimulus_table}")
    for end_frame in stimulus_table.end_frame.values:
        if not np.isnan(end_frame):
            end_time.append(stimulus_timestamps[int(end_frame)])
        else:
            end_time.append(float("nan"))

    stimulus_table.insert(loc=4, column="stop_time", value=end_time)
    stimulus_table.set_index("stimulus_presentations_id", inplace=True)
    stimulus_table = stimulus_table[sorted(stimulus_table.columns)]
    return stimulus_table


def get_images_dict(pkl_dict) -> Dict:
    """
    Gets the dictionary of images that were presented during an experiment
    along with image set metadata and the image specific metadata. This
    function uses the path to the image pkl file to read the images and their
    metadata from the pkl file and return this dictionary.
    Parameters
    ----------
    pkl_dict: The pkl file containing the data for the stimuli presented during
         experiment

    Returns
    -------
    Dict:
        A dictionary containing keys images, metadata, and image_attributes.
        These correspond to paths to image arrays presented, metadata
        on the whole set of images, and metadata on specific images,
        respectively.

    """
    # Sometimes the source is a zipped pickle:
    pkl_stimuli = pkl_dict["items"]["behavior"]["stimuli"]
    metadata = {"image_set": pkl_stimuli["images"]["image_path"]}

    # Get image file name;
    # These are encoded case-insensitive in the pickle file :/
    filename = stim.convert_filepath_caseinsensitive(metadata["image_set"])

    image_set = pkl.load_img_pkl(open(filename, "rb"))
    images = []
    images_meta = []

    for cat, cat_images in image_set.items():
        cat_decoded = cat.decode("utf-8")
        for img_index, (img_name, img) in enumerate(cat_images.items()):
            meta = {
                "image_category": cat_decoded,
                "image_name": img_name.decode("utf-8"),
                "orientation": np.NaN,
                "phase": np.NaN,
                "spatial_frequency": np.NaN,
                "image_index": img_index,
            }

            images.append(img)
            images_meta.append(meta)

    images_dict = dict(
        metadata=metadata,
        images=images,
        image_attributes=images_meta,
    )

    return images_dict


def get_gratings_metadata(stimuli: Dict, start_idx: int = 0) -> pd.DataFrame:
    """
    This function returns the metadata for each unique grating that was
    presented during the experiment. If no gratings were displayed during
    this experiment it returns an empty dataframe with the expected columns.
    Parameters
    ----------
    stimuli:
        The stimuli field (pkl['items']['behavior']['stimuli']) loaded
        from the experiment pkl file.
    start_idx:
        The index to start index column

    Returns
    -------
    pd.DataFrame:
        DataFrame containing the unique stimuli presented during an
        experiment. The columns contained in this DataFrame are
        'image_category', 'image_name', 'image_set', 'phase',
        'spatial_frequency', 'orientation', and 'image_index'.
        This returns empty if no gratings were presented.

    """
    if stimuli.get("grating"):
        phase = stimuli["grating"]["phase"]
        correct_freq = stimuli["grating"]["sf"]
        set_logs = stimuli["grating"]["set_log"]
        unique_oris = set([set_log[1] for set_log in set_logs])

        image_names = []

        for unique_ori in unique_oris:
            image_names.append(f"gratings_{float(unique_ori)}")

        grating_dict = {
            "image_category": ["grating"] * len(unique_oris),
            "image_name": image_names,
            "orientation": list(unique_oris),
            "image_set": ["grating"] * len(unique_oris),
            "phase": [phase] * len(unique_oris),
            "spatial_frequency": [correct_freq] * len(unique_oris),
            "image_index": range(start_idx, start_idx + len(unique_oris), 1),
        }
        grating_df = pd.DataFrame.from_dict(grating_dict)
    else:
        grating_df = pd.DataFrame(
            columns=[
                "image_category",
                "image_name",
                "image_set",
                "phase",
                "spatial_frequency",
                "orientation",
                "image_index",
            ]
        )
    return grating_df


def get_stimulus_metadata(pkl) -> pd.DataFrame:
    """
    Gets the stimulus metadata for each type of stimulus presented during
    the experiment. The metadata is return for gratings, images, and omitted
    stimuli.
    Parameters
    ----------
    pkl: the pkl file containing the information about what stimuli were
         presented during the experiment

    Returns
    -------
    pd.DataFrame:
        The dataframe containing a row for every stimulus that was presented
        during the experiment. The row contains the following data,
        image_category, image_name, image_set, phase, spatial_frequency,
        orientation, and image index.

    """
    stimuli = pkl["items"]["behavior"]["stimuli"]
    if "images" in stimuli:
        images = get_images_dict(pkl)
        stimulus_index_df = pd.DataFrame(images["image_attributes"])
        image_set_filename = stim.convert_filepath_caseinsensitive(
            images["metadata"]["image_set"]
        )
        stimulus_index_df["image_set"] = stim.get_image_set_name(
            image_set_path=image_set_filename
        )
    else:
        stimulus_index_df = pd.DataFrame(
            columns=[
                "image_name",
                "image_category",
                "image_set",
                "phase",
                "spatial_frequency",
                "image_index",
            ]
        )
        stimulus_index_df = stimulus_index_df.astype(
            {
                "image_name": str,
                "image_category": str,
                "image_set": str,
                "phase": float,
                "spatial_frequency": float,
                "image_index": int,
            }
        )

    # get the grating metadata will be empty if gratings are absent
    grating_df = get_gratings_metadata(
        stimuli, start_idx=len(stimulus_index_df)
    )
    stimulus_index_df = pd.concat(
        [stimulus_index_df, grating_df], ignore_index=True, sort=False
    )

    # Add an entry for omitted stimuli
    omitted_df = pd.DataFrame(
        {
            "image_category": ["omitted"],
            "image_name": ["omitted"],
            "image_set": ["omitted"],
            "orientation": np.NaN,
            "phase": np.NaN,
            "spatial_frequency": np.NaN,
            "image_index": len(stimulus_index_df),
        }
    )
    stimulus_index_df = pd.concat(
        [stimulus_index_df, omitted_df], ignore_index=True, sort=False
    )
    stimulus_index_df.set_index(["image_index"], inplace=True, drop=True)
    return stimulus_index_df


def get_stimulus_epoch(
    set_log: List[Tuple[str, Union[str, int], int, int]],
    current_set_index: int,
    start_frame: int,
    n_frames: int,
) -> Tuple[int, int]:
    """
    Gets the frame range for which a stimuli was presented and the transition
    to the next stimuli was ongoing. Returns this in the form of a tuple.
    Parameters
    ----------
    set_log: List[Tuple[str, Union[str, int], int, int
        The List of Tuples in the form of
        (stimuli_type ('Image' or 'Grating'),
         stimuli_descriptor (image_name or orientation of grating in degrees),
         nonsynced_time_of_display (not sure, it's never used),
         display_frame (frame that stimuli was displayed))
    current_set_index: int
        Index of stimuli set to calculate window
    start_frame: int
        frame where stimuli was set, set_log[current_set_index][3]
    n_frames: int
        number of frames for which stimuli were displayed

    Returns
    -------
    Tuple[int, int]:
        A tuple where index 0 is start frame of stimulus window and index 1 is
        end frame of stimulus window

    """
    try:
        next_set_event = set_log[current_set_index + 1]
    except IndexError:  # assume this is the last set event
        next_set_event = (
            None,
            None,
            None,
            n_frames,
        )

    return start_frame, next_set_event[3]  # end frame isn't inclusive


def get_draw_epochs(
    draw_log: List[int], start_frame: int, stop_frame: int
) -> List[Tuple[int, int]]:
    """
    Gets the frame numbers of the active frames within a stimulus window.
    Stimulus epochs come in the form [0, 0, 1, 1, 0, 0] where the stimulus is
    active for some amount of time in the window indicated by int 1 at that
    frame. This function returns the ranges for which the set_log is 1 within
    the draw_log window.
    Parameters
    ----------
    draw_log: List[int]
        A list of ints indicating for what frames stimuli were active
    start_frame: int
        The start frame to search within the draw_log for active values
    stop_frame: int
        The end frame to search within the draw_log for active values

    Returns
    -------
    List[Tuple[int, int]]
        A list of tuples indicating the start and end frames of every
        contiguous set of active values within the specified window
        of the draw log.
    """
    draw_epochs = []
    current_frame = start_frame

    while current_frame <= stop_frame:
        epoch_length = 0
        while current_frame < stop_frame and draw_log[current_frame] == 1:
            epoch_length += 1
            current_frame += 1
        else:
            current_frame += 1

        if epoch_length:
            draw_epochs.append(
                (
                    current_frame - epoch_length - 1,
                    current_frame - 1,
                )
            )

    return draw_epochs


def unpack_change_log(change):
    """
    Unpacks the change log into a dictionary containing the category of the
    stimuli that was changed, the name of the stimuli that was changed, the
    category of the stimuli that the change was made to, the name of the
    stimuli that the change was made to, the time of the change, and the frame
    of the change.

    Parameters
    ----------
    change: Tuple[str, str, str, str, int, int]
        A tuple containing the category of the stimuli that was changed

    Returns
    -------
    Dict:
        A dictionary containing the category of the stimuli that was changed,
        the name of the stimuli that was changed, the category of the stimuli
        that the change was made to, the name of the stimuli that the change
        was made to, the time of the change, and the frame of the change.
    """

    (
        (from_category, from_name),
        (
            to_category,
            to_name,
        ),
        time,
        frame,
    ) = change

    return dict(
        frame=frame,
        time=time,
        from_category=from_category,
        to_category=to_category,
        from_name=from_name,
        to_name=to_name,
    )


def get_visual_stimuli_df(data, time) -> pd.DataFrame:
    """
    This function loads the stimuli and the omitted stimuli into a dataframe.
    These stimuli are loaded from the input data, where the set_log and
    draw_log contained within are used to calculate the epochs. These epochs
    are used as start_frame and end_frame and converted to times by input
    stimulus timestamps. The omitted stimuli do not have a end_frame by design
    though there duration is always 250ms.
    :param data: the behavior data file
    :param time: the stimulus timestamps indicating when each stimuli is
                 displayed
    :return: df: a pandas dataframe containing the stimuli and omitted stimuli
                 that were displayed with their frame, end_frame, start_time,
                 and duration
    """
    try:
        stimuli = data["items"]["behavior"]["stimuli"]
    except KeyError:
        stimuli = data["items"]["foraging"]["stimuli"]
    n_frames = len(time)
    visual_stimuli_data = []
    for stim_dict in stimuli.values():
        for idx, (attr_name, attr_value, _, frame) in enumerate(
            stim_dict["set_log"]
        ):
            orientation = attr_value if attr_name.lower() == "ori" else np.nan
            image_name = attr_value if attr_name.lower() == "image" else np.nan

            stimulus_epoch = get_stimulus_epoch(
                stim_dict["set_log"],
                idx,
                frame,
                n_frames,
            )
            draw_epochs = get_draw_epochs(
                stim_dict["draw_log"], *stimulus_epoch
            )

            for epoch_start, epoch_end in draw_epochs:
                visual_stimuli_data.append(
                    {
                        "orientation": orientation,
                        "image_name": image_name,
                        "frame": epoch_start,
                        "end_frame": epoch_end,
                        "time": time[epoch_start],
                        "duration": time[epoch_end] - time[epoch_start],
                        # this will always work because an epoch
                        # will never occur near the end of time
                        "omitted": False,
                    }
                )

    visual_stimuli_df = pd.DataFrame(data=visual_stimuli_data)

    # Add omitted flash info:
    try:
        omitted_flash_frame_log = data["items"]["behavior"][
            "omitted_flash_frame_log"
        ]
    except KeyError:
        # For sessions for which there were no omitted flashes
        omitted_flash_frame_log = dict()

    omitted_flash_list = []
    for _, omitted_flash_frames in omitted_flash_frame_log.items():
        stim_frames = visual_stimuli_df["frame"].values
        omitted_flash_frames = np.array(omitted_flash_frames)

        # Test offsets of omitted flash frames
        # to see if they are in the stim log
        offsets = np.arange(-3, 4)
        offset_arr = np.add(
            np.repeat(
                omitted_flash_frames[:, np.newaxis], offsets.shape[0], axis=1
            ),
            offsets,
        )
        matched_any_offset = np.any(np.isin(offset_arr, stim_frames), axis=1)

        #  Remove omitted flashes that also exist in the stimulus log
        was_true_omitted = np.logical_not(matched_any_offset)  # bool
        omitted_flash_frames_to_keep = omitted_flash_frames[was_true_omitted]

        # Have to remove frames that are double-counted in omitted log
        omitted_flash_list += list(np.unique(omitted_flash_frames_to_keep))

    omitted = np.ones_like(omitted_flash_list).astype(bool)
    time = [time[fi] for fi in omitted_flash_list]
    omitted_df = pd.DataFrame(
        {
            "omitted": omitted,
            "frame": omitted_flash_list,
            "time": time,
            "image_name": "omitted",
        }
    )

    df = (
        pd.concat((visual_stimuli_df, omitted_df), sort=False)
        .sort_values("frame")
        .reset_index()
    )
    return df


def get_image_names(behavior_stimulus_file) -> Set[str]:
    """Gets set of image names shown during behavior session"""
    stimuli = behavior_stimulus_file["stimuli"]
    image_names = set()
    for stim_dict in stimuli.values():
        for attr_name, attr_value, _, _ in stim_dict["set_log"]:
            if attr_name.lower() == "image":
                image_names.add(attr_value)
    return image_names


def is_change_event(stimulus_presentations: pd.DataFrame) -> pd.Series:
    """
    Returns whether a stimulus is a change stimulus
    A change stimulus is defined as the first presentation of a new image_name
    Omitted stimuli are ignored
    The first stimulus in the session is ignored

    :param stimulus_presentations
        The stimulus presentations table

    :return: is_change: pd.Series indicating whether a given stimulus is a
        change stimulus
    """
    stimuli = stimulus_presentations["image_name"]

    # exclude omitted stimuli
    stimuli = stimuli[~stimulus_presentations["omitted"]]

    prev_stimuli = stimuli.shift()

    # exclude first stimulus
    stimuli = stimuli.iloc[1:]
    prev_stimuli = prev_stimuli.iloc[1:]

    is_change = stimuli != prev_stimuli

    # reset back to original index
    is_change = is_change.reindex(stimulus_presentations.index).rename(
        "is_change"
    )

    # Excluded stimuli are not change events
    is_change = is_change.fillna(False)

    return is_change


def get_flashes_since_change(
    stimulus_presentations: pd.DataFrame,
) -> pd.Series:
    """Calculate the number of times an images is flashed between changes.

    Parameters
    ----------
    stimulus_presentations : pandas.DataFrame
        Table of presented stimuli with ``is_change`` column already
        calculated.

    Returns
    -------
    flashes_since_change : pandas.Series
        Number of times the same image is flashed between image changes.
    """
    flashes_since_change = pd.Series(
        data=np.zeros(len(stimulus_presentations), dtype=float),
        index=stimulus_presentations.index,
        name="flashes_since_change",
        dtype="int",
    )
    for idx, (pd_index, row) in enumerate(stimulus_presentations.iterrows()):
        omitted = row["omitted"]
        if pd.isna(row["omitted"]):
            omitted = False
        if row["image_name"] == "omitted" or omitted:
            flashes_since_change.iloc[idx] = flashes_since_change.iloc[idx - 1]
        else:
            if row["is_change"] or idx == 0:
                flashes_since_change.iloc[idx] = 0
            else:
                flashes_since_change.iloc[idx] = (
                    flashes_since_change.iloc[idx - 1] + 1
                )
    return flashes_since_change


def add_active_flag(
    stim_pres_table: pd.DataFrame, trials: pd.DataFrame
) -> pd.DataFrame:
    """Mark the active stimuli by lining up the stimulus times with the
    trials times.

    Parameters
    ----------
    stim_pres_table : pandas.DataFrame
        Stimulus table to add active column to.
    trials : pandas.DataFrame
        Trials table to align with the stimulus table.

    Returns
    -------
    stimulus_table : pandas.DataFrame
        Copy of ``stim_pres_table`` with added acive column.
    """
    if "active" in stim_pres_table.columns:
        return stim_pres_table
    else:
        active = pd.Series(
            data=np.zeros(len(stim_pres_table), dtype=bool),
            index=stim_pres_table.index,
            name="active",
        )
        stim_mask = (
            (stim_pres_table.start_time > trials.start_time.min())
            & (stim_pres_table.start_time < trials.stop_time.max())
            & (~stim_pres_table.image_name.isna())
        )
        active[stim_mask] = True

        # Clean up potential stimuli that fall outside in time of the trials
        # but are part of the "active" stimulus block.
        if "stimulus_block" in stim_pres_table.columns:
            for stim_block in stim_pres_table["stimulus_block"].unique():
                block_mask = stim_pres_table["stimulus_block"] == stim_block
                if np.any(active[block_mask]):
                    active[block_mask] = True
        stim_pres_table["active"] = active
        return stim_pres_table


def compute_trials_id_for_stimulus(
    stim_pres_table: pd.DataFrame, trials_table: pd.DataFrame
) -> pd.Series:
    """Add an id to allow for merging of the stimulus presentations
    table with the trials table.

    If stimulus_block is not available as a column in the input table, return
    an empty set of trials_ids.

    Parameters
    ----------
    stim_pres_table : pandas.DataFrame
        Pandas stimulus table to create trials_id from.
    trials_table : pandas.DataFrame
        Trials table to create id from using trial start times.

    Returns
    -------
    trials_ids : pd.Series
        Unique id to allow merging of the stim table with the trials table.
        Null values are represented by -1.

    Note
    ----
    ``trials_id`` values are copied from active stimulus blocks into
    passive stimulus/replay blocks that contain the same image ordering and
    length.
    """
    # Create a placeholder for the trials_id.
    trials_ids = pd.Series(
        data=np.full(len(stim_pres_table), INT_NULL, dtype=int),
        index=stim_pres_table.index,
        name="trials_id",
    ).astype("int")

    # Find stimulus blocks that start within a trial. Copy the trial_id
    # into our new trials_ids series. For some sessions there are gaps in
    # between one trial's end and the next's stop time so we account for this
    # by only using the max time for all trials as the limit.
    max_trials_stop = trials_table.stop_time.max()
    for idx, trial in trials_table.iterrows():
        stim_mask = (
            (stim_pres_table.start_time > trial.start_time)
            & (stim_pres_table.start_time < max_trials_stop)
            & (~stim_pres_table.image_name.isna())
        )
        trials_ids[stim_mask] = idx

    # Return input frame if the stimulus_block or active is not available.
    if (
        "stimulus_block" not in stim_pres_table.columns
        or "active" not in stim_pres_table.columns
    ):
        return trials_ids
    active_sorted = stim_pres_table.active

    # The code below finds all stimulus blocks that contain images/trials
    # and attempts to detect blocks that are identical to copy the associated
    # trials_ids into those blocks. In the parlance of the data this is
    # copying the active stimulus block data into the passive stimulus block.

    # Get the block ids for the behavior trial presentations
    stim_blocks = stim_pres_table.stimulus_block
    stim_image_names = stim_pres_table.image_name
    active_stim_blocks = stim_blocks[active_sorted].unique()
    # Find passive blocks that show images for potential copying of the active
    # into a passive stimulus block.
    passive_stim_blocks = stim_blocks[
        np.logical_and(~active_sorted, ~stim_image_names.isna())
    ].unique()

    # Copy the trials_id into the passive block if it exists.
    if len(passive_stim_blocks) > 0:
        for active_stim_block in active_stim_blocks:
            active_block_mask = stim_blocks == active_stim_block
            active_images = stim_image_names[active_block_mask].values
            for passive_stim_block in passive_stim_blocks:
                passive_block_mask = stim_blocks == passive_stim_block
                if np.array_equal(
                    active_images, stim_image_names[passive_block_mask].values
                ):
                    trials_ids.loc[passive_block_mask] = trials_ids[
                        active_block_mask
                    ].values

    return trials_ids.sort_index()


def fix_omitted_end_frame(stim_pres_table: pd.DataFrame) -> pd.DataFrame:
    """Fill NaN ``end_frame`` values for omitted frames.

    Additionally, change type of ``end_frame`` to int.

    Parameters
    ----------
    stim_pres_table : `pandas.DataFrame`
        Input stimulus table to fix/fill omitted ``end_frame`` values.

    Returns
    -------
    output : `pandas.DataFrame`
        Copy of input DataFrame with filled omitted, ``end_frame`` values and
        fixed typing.
    """
    median_stim_frame_duration = np.nanmedian(
        stim_pres_table["end_frame"] - stim_pres_table["start_frame"]
    )
    omitted_end_frames = (
        stim_pres_table[stim_pres_table["omitted"]]["start_frame"]
        + median_stim_frame_duration
    )
    stim_pres_table.loc[
        stim_pres_table["omitted"], "end_frame"
    ] = omitted_end_frames

    stim_dtypes = stim_pres_table.dtypes.to_dict()
    stim_dtypes["start_frame"] = int
    stim_dtypes["end_frame"] = int

    return stim_pres_table.astype(stim_dtypes)


def compute_is_sham_change(
    stim_df: pd.DataFrame, trials: pd.DataFrame
) -> pd.DataFrame:
    """Add is_sham_change to stimulus presentation table.

    Parameters
    ----------
    stim_df : pandas.DataFrame
        Stimulus presentations table to add is_sham_change to.
    trials : pandas.DataFrame
        Trials data frame to pull info from to create

    Returns
    -------
    stimulus_presentations : pandas.DataFrame
        Input ``stim_df`` DataFrame with the is_sham_change column added.
    """
    if (
        "trials_id" not in stim_df.columns
        or "active" not in stim_df.columns
        or "stimulus_block" not in stim_df.columns
    ):
        return stim_df
    stim_trials = stim_df.merge(
        trials, left_on="trials_id", right_index=True, how="left"
    )
    catch_frames = stim_trials[stim_trials["catch"].fillna(False)][
        "change_frame"
    ].unique()

    stim_df["is_sham_change"] = False
    catch_flashes = stim_df[
        stim_df["start_frame"].isin(catch_frames)
    ].index.values
    stim_df.loc[catch_flashes, "is_sham_change"] = True

    stim_blocks = stim_df.stimulus_block
    stim_image_names = stim_df.image_name
    active_stim_blocks = stim_blocks[stim_df.active].unique()
    # Find passive blocks that show images for potential copying of the active
    # into a passive stimulus block.
    passive_stim_blocks = stim_blocks[
        np.logical_and(~stim_df.active, ~stim_image_names.isna())
    ].unique()

    # Copy the trials_id into the passive block if it exists.
    if len(passive_stim_blocks) > 0:
        for active_stim_block in active_stim_blocks:
            active_block_mask = stim_blocks == active_stim_block
            active_images = stim_image_names[active_block_mask].values
            for passive_stim_block in passive_stim_blocks:
                passive_block_mask = stim_blocks == passive_stim_block
                if np.array_equal(
                    active_images, stim_image_names[passive_block_mask].values
                ):
                    stim_df.loc[
                        passive_block_mask, "is_sham_change"
                    ] = stim_df[active_block_mask]["is_sham_change"].values

    return stim_df.sort_index()


def fingerprint_from_stimulus_file(
    stimulus_presentations: pd.DataFrame, stimulus_file, stimulus_timestamps
):
    """
    Instantiates `fingerprintStimulus` from stimulus file

    Parameters
    ----------
    stimulus_presentations:
        Table containing previous stimuli
    stimulus_file
        BehaviorStimulusFile
    stimulus_timestamps
        StimulusTimestamps

    Returns
    -------
    `fingerprintStimulus`
        Instantiated fingerprintStimulus
    """
    fingerprint_stim = stimulus_file["items"]["behavior"]["items"][
        "fingerprint"
    ]["static_stimulus"]

    n_repeats = fingerprint_stim["runs"]

    # spontaneous + fingerprint indices relative to start of session
    stimulus_session_frame_indices = np.array(
        stimulus_file["items"]["behavior"]["items"]["fingerprint"][
            "frame_indices"
        ]
    )

    movie_length = int(len(fingerprint_stim["sweep_frames"]) / n_repeats)

    # Start index within the spontaneous + fingerprint block
    movie_start_index = sum(
        1 for frame in fingerprint_stim["frame_list"] if frame == -1
    )
    res = []
    for repeat in range(n_repeats):
        for frame in range(movie_length):
            # 0-indexed frame indices relative to start of fingerprint
            # movie
            stimulus_frame_indices = np.array(
                fingerprint_stim["sweep_frames"][
                    frame + (repeat * movie_length)
                ]
            )
            start_frame, end_frame = stimulus_session_frame_indices[
                stimulus_frame_indices + movie_start_index
            ]
            start_time = stimulus_timestamps[start_frame]
            stop_time = stimulus_timestamps[
                min(end_frame + 1, len(stimulus_timestamps) - 1)
            ]
            res.append(
                {
                    "movie_frame_index": frame,
                    "start_time": start_time,
                    "stop_time": stop_time,
                    "start_frame": start_frame,
                    "end_frame": end_frame,
                    "movie_repeat": repeat,
                    "duration": stop_time - start_time,
                }
            )
    table = pd.DataFrame(res)

    table["stim_block"] = (
        stimulus_presentations["stim_block"].max() + 2
    )  # + 2 since there is a gap before this stimulus
    table["stim_name"] = "natural_movie_one"

    table = table.astype(
        {c: "int64" for c in table.select_dtypes(include="int")}
    )

    return table


def from_stimulus_file(
    stimulus_file,
    stimulus_timestamps,
    limit_to_images: Optional[List] = None,
    column_list: Optional[List[str]] = None,
    fill_omitted_values: bool = True,
    project_code: Optional[str] = None,
):
    """Get stimulus presentation data.

    Parameters
    ----------
    stimulus_file : BehaviorStimulusFile
        Input stimulus_file to create presentations dataframe from.
    stimulus_timestamps : StimulusTimestamps
        Timestamps of the stimuli
    behavior_session_id : int
        LIMS id of behavior session
    trials: Trials
        Object to create trials_id column in Presentations table
        allowing for mering of the two tables.
    limit_to_images : Optional, list of str
        Only return images given by these image names
    column_list : Optional, list of str
        The columns and order of columns in the final dataframe
    fill_omitted_values : Optional, bool
        Whether to fill stop_time and duration for omitted frames
    project_code: Optional, ProjectCode
        For released datasets, provide a project code
        to produce explicitly named stimulus_block column values in the
        column stimulus_block_name

    Returns
    -------
    output_presentations: Presentations
        Object with a table whose rows are stimulus presentations
        (i.e. a given image, for a given duration, typically 250 ms)
        and whose columns are presentation characteristics.
    """
    data = pkl.load_pkl(stimulus_file)
    raw_stim_pres_df = get_stimulus_presentations(data, stimulus_timestamps)
    raw_stim_pres_df = raw_stim_pres_df.drop(columns=["index"])
    raw_stim_pres_df = check_for_errant_omitted_stimulus(
        input_df=raw_stim_pres_df
    )

    # Fill in nulls for image_name
    # This makes two assumptions:
    #   1. Nulls in `image_name` should be "gratings_<orientation>"
    #   2. Gratings are only present (or need to be fixed) when all
    #      values for `image_name` are null.
    if pd.isnull(raw_stim_pres_df["image_name"]).all():
        if ~pd.isnull(raw_stim_pres_df["orientation"]).all():
            raw_stim_pres_df["image_name"] = raw_stim_pres_df[
                "orientation"
            ].apply(lambda x: f"gratings_{x}")
        else:
            raise ValueError(
                "All values for 'orientation' and " "'image_name are null."
            )

    stimulus_metadata_df = get_stimulus_metadata(data)

    idx_name = raw_stim_pres_df.index.name
    if idx_name is None:
        return raw_stim_pres_df

    stimulus_index_df = (
        raw_stim_pres_df.reset_index()
        .merge(stimulus_metadata_df.reset_index(), on=["image_name"])
        .set_index(idx_name)
    )

    stimulus_index_df = (
        stimulus_index_df[
            [
                "image_set",
                "image_index",
                "start_time",
                "phase",
                "spatial_frequency",
            ]
        ]
        .rename(columns={"start_time": "timestamps"})
        .sort_index()
        .set_index("timestamps", drop=True)
    )
    stimulus_index_df["image_index"] = stimulus_index_df["image_index"].astype(
        "int"
    )
    stim_pres_df = raw_stim_pres_df.merge(
        stimulus_index_df,
        left_on="start_time",
        right_index=True,
        how="left",
    )
    if len(raw_stim_pres_df) != len(stim_pres_df):
        raise ValueError(
            "Length of `stim_pres_df` should not change after"
            f" merge; was {len(raw_stim_pres_df)}, now "
            f" {len(stim_pres_df)}."
        )

    stim_pres_df["is_change"] = is_change_event(
        stimulus_presentations=stim_pres_df
    )
    stim_pres_df["flashes_since_change"] = get_flashes_since_change(
        stimulus_presentations=stim_pres_df
    )

    # Sort columns then drop columns which contain only all NaN values
    stim_pres_df = stim_pres_df[sorted(stim_pres_df)].dropna(axis=1, how="all")
    if limit_to_images is not None:
        stim_pres_df = stim_pres_df[
            stim_pres_df["image_name"].isin(limit_to_images)
        ]
        stim_pres_df.index = pd.Index(
            range(stim_pres_df.shape[0]), name=stim_pres_df.index.name
        )

    stim_pres_df["stim_block"] = 0
    stim_pres_df["stim_name"] = get_stimulus_name(data)

    stim_pres_df = fix_omitted_end_frame(stim_pres_df)

    has_fingerprint_stimulus = (
        "fingerprint" in data["items"]["behavior"]["items"]
    )
    if has_fingerprint_stimulus:
        stim_pres_df = add_fingerprint_stimulus(
            stimulus_presentations=stim_pres_df,
            stimulus_file=data,
            stimulus_timestamps=stimulus_timestamps,
        )
    stim_pres_df = postprocess(
        presentations=stim_pres_df,
        fill_omitted_values=fill_omitted_values,
        coerce_bool_to_boolean=True,
    )

    return (stim_pres_df, column_list)


def get_is_image_novel(
    image_names: List[str],
    behavior_session_id: int,
) -> Dict[str, bool]:
    """
    Returns whether each image in `image_names` is novel for the mouse

    Parameters
    ----------
    image_names:
        List of image names
    behavior_session_id
        LIMS behavior session id

    Returns
    -------
    Dict mapping image name to is_novel
    """

    # TODO: FIND A WAY TO DO THIS WITHOUT LIMS?

    return False
    """
    mouse = Mouse.from_behavior_session_id(
        behavior_session_id=behavior_session_id
    )
    prior_images_shown = mouse.get_images_shown(
        up_to_behavior_session_id=behavior_session_id
    )

    image_names = set(
        [x for x in image_names if x != "omitted" and type(x) is str]
    )
    is_novel = {
        f"{image_name}": image_name not in prior_images_shown
        for image_name in image_names
    }
    return is_novel
    """


def postprocess(
    presentations: pd.DataFrame,
    fill_omitted_values=True,
    coerce_bool_to_boolean=True,
    omitted_time_duration: float = 0.25,
) -> pd.DataFrame:
    """
    Applies further processing to `presentations`

    Parameters
    ----------
    presentations
        Presentations df
    fill_omitted_values
        Whether to fill stop time and duration for omitted flashes
    coerce_bool_to_boolean
        Whether to coerce columns of "Object" dtype that are truly bool
        to nullable "boolean" dtype
    omitted_time_duration
        Amount of time a stimuli is omitted for in seconds"""
    df = presentations
    if fill_omitted_values:
        fill_missing_values_for_omitted_flashes(
            df=df, omitted_time_duration=omitted_time_duration
        )
    if coerce_bool_to_boolean:
        df = df.astype(
            {
                c: "boolean"
                for c in df.select_dtypes("O")
                if set(df[c][~df[c].isna()].unique()).issubset({True, False})
            }
        )
    df = check_for_errant_omitted_stimulus(input_df=df)
    return df


def check_for_errant_omitted_stimulus(
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    """Check if the first entry in the DataFrame is an omitted stimulus.

    This shouldn't happen and likely reflects some sort of camstim error
    with appending frames to the omitted flash frame log. See
    explanation here:
    https://github.com/AllenInstitute/AllenSDK/issues/2577

    Parameters
    ----------/
    input_df : DataFrame
        Input stimulus table to check for "omitted" stimulus.

    Returns
    -------
    modified_df : DataFrame
        Dataframe with omitted stimulus removed from first row or if not
        found, return input_df unmodified.
    """

    def safe_omitted_check(input_df: pd.Series, stimulus_block: Optional[int]):
        """
        Check if the first row in the input_df is an omitted stimulus.

        Parameters
        ----------
        input_df : pd.Series
            Input stimulus table to check for "omitted" stimulus.
        stimulus_block : Optional[int]
            Stimulus block to check for omitted stimulus in.

        Returns
        -------
        input_df : pd.Series
            Dataframe with omitted stimulus removed from first row or if not
        """
        if stimulus_block is not None:
            first_row = input_df[
                input_df["stimulus_block"] == stim_block
            ].iloc[0]
        else:
            first_row = input_df.iloc[0]

        if not pd.isna(first_row["omitted"]):
            if first_row["omitted"]:
                input_df = input_df.drop(first_row.name, axis=0)
        return input_df

    if "omitted" in input_df.columns and len(input_df) > 0:
        if "stimulus_block" in input_df.columns:
            for stim_block in input_df["stimulus_block"].unique():
                input_df = safe_omitted_check(
                    input_df=input_df, stimulus_block=stim_block
                )
        else:
            input_df = safe_omitted_check(
                input_df=input_df, stimulus_block=None
            )
    return input_df


def fill_missing_values_for_omitted_flashes(
    df: pd.DataFrame, omitted_time_duration: float = 0.25
) -> pd.DataFrame:
    """
    This function sets the stop time for a row that is an omitted
    stimulus. An omitted stimulus is a stimulus where a mouse is
    shown only a grey screen and these last for 250 milliseconds.
    These do not include a stop_time or end_frame like other stimuli in
    the stimulus table due to design choices.

    Parameters
    ----------
    df
        Stimuli presentations dataframe
    omitted_time_duration
        Amount of time a stimulus is omitted for in seconds
    """
    omitted = df["omitted"].fillna(False)
    df.loc[omitted, "stop_time"] = (
        df.loc[omitted, "start_time"] + omitted_time_duration
    )
    df.loc[omitted, "duration"] = omitted_time_duration
    return df


def get_spontaneous_stimulus(
    stimulus_presentations_table: pd.DataFrame,
) -> pd.DataFrame:
    """The spontaneous stimulus is a gray screen shown in between
    different stimulus blocks. This method finds any gaps in the stimulus
    presentations. These gaps are assumed to be spontaneous stimulus.

    Parameters
    ---------
    stimulus_presentations_table : pd.DataFrame
        Input stimulus presentations table.

    Returns
    -------
    output_frame : pd.DataFrame
        stimulus_presentations_table with added spotaneous stimulus blocks
        added.

    Raises
    ------
    RuntimeError if there are any gaps in stimulus blocks > 1
    """
    res = []
    # Check for 5 minute gray screen stimulus block at the start of the
    # movie. We give some leeway around 5 minutes at 285 seconds to account
    # for some sessions which have start times slightly less than 300
    # seconds. This also makes sure that presentations that start slightly
    # late are not erroneously added as a "grey screen".
    if (
        stimulus_presentations_table.iloc[0]["start_frame"] > 0
        and stimulus_presentations_table.iloc[0]["start_time"] > 285
    ):
        res.append(
            {
                "duration": stimulus_presentations_table.iloc[0]["start_time"],
                "start_time": 0,
                "stop_time": stimulus_presentations_table.iloc[0][
                    "start_time"
                ],
                "start_frame": 0,
                "end_frame": stimulus_presentations_table.iloc[0][
                    "start_frame"
                ],
                "stim_block": 0,
                "stim_name": "spontaneous",
            }
        )
        # Increment the stimulus blocks by 1 to to account for the
        # new stimulus at the start of the file.
        stimulus_presentations_table["stim_block"] += 1

    spontaneous_stimulus_blocks = get_spontaneous_block_indices(
        stimulus_blocks=(stimulus_presentations_table["stim_block"].values)
    )

    for spontaneous_block in spontaneous_stimulus_blocks:
        prev_stop_time = stimulus_presentations_table[
            stimulus_presentations_table["stim_block"] == spontaneous_block - 1
        ]["stop_time"].max()
        prev_end_frame = stimulus_presentations_table[
            stimulus_presentations_table["stim_block"] == spontaneous_block - 1
        ]["end_frame"].max()
        next_start_time = stimulus_presentations_table[
            stimulus_presentations_table["stim_block"] == spontaneous_block + 1
        ]["start_time"].min()
        next_start_frame = stimulus_presentations_table[
            stimulus_presentations_table["stim_block"] == spontaneous_block + 1
        ]["start_frame"].min()
        res.append(
            {
                "duration": next_start_time - prev_stop_time,
                "start_time": prev_stop_time,
                "stop_time": next_start_time,
                "start_frame": prev_end_frame,
                "end_frame": next_start_frame,
                "stim_block": spontaneous_block,
                "stim_name": "spontaneous",
            }
        )

    res = pd.DataFrame(res)

    return pd.concat([stimulus_presentations_table, res]).sort_values(
        "start_frame"
    )


def add_fingerprint_stimulus(
    stimulus_presentations: pd.DataFrame,
    stimulus_file,
    stimulus_timestamps,
) -> pd.DataFrame:
    """Adds the fingerprint stimulus and the preceding gray screen to
    the stimulus presentations table

    Returns
    -------
    pd.DataFrame: stimulus presentations with gray screen + fingerprint
    movie added"""

    fingerprint_stimulus = fingerprint_from_stimulus_file(
        stimulus_presentations=stimulus_presentations,
        stimulus_file=stimulus_file,
        stimulus_timestamps=stimulus_timestamps,
    )

    stimulus_presentations = pd.concat(
        [stimulus_presentations, fingerprint_stimulus]
    )
    stimulus_presentations = get_spontaneous_stimulus(
        stimulus_presentations_table=stimulus_presentations
    )

    # reset index to go from 0...end
    stimulus_presentations.index = pd.Index(
        np.arange(0, stimulus_presentations.shape[0]),
        name=stimulus_presentations.index.name,
        dtype=stimulus_presentations.index.dtype,
    )
    return stimulus_presentations


def get_spontaneous_block_indices(stimulus_blocks: np.ndarray) -> np.ndarray:
    """Gets the indices where there is a gap in stimulus block. This is
    where spontaneous blocks are.
    Example: stimulus blocks are [0, 2, 3]. There is a spontaneous block at 1.

    Parameters
    ----------
    stimulus_blocks: Stimulus blocks in the stimulus presentations table

    Notes
    -----
    This doesn't support a spontaneous block appearing at the beginning or
    end of a session

    Returns
    -------
    np.array: spontaneous stimulus blocks
    """
    blocks = np.sort(np.unique(stimulus_blocks))
    block_diffs = np.diff(blocks)
    if (block_diffs > 2).any():
        raise RuntimeError(
            f"There should not be any stimulus block "
            f"diffs greater than 2. The stimulus "
            f"blocks are {blocks}"
        )

    # i.e. if the current blocks are [0, 2], then block_diffs will
    # be [2], with a gap (== 2) at index 0, meaning that the spontaneous block
    # is at index 1
    block_indices = blocks[np.where(block_diffs == 2)[0]] + 1
    return block_indices


def get_stimulus_name(stim_file) -> str:
    """
    Get the image stimulus name by parsing the file path of the image set.

    If no image set, check for gratings and return "behavior" if not found.

    Parameters
    ----------
    stimulus_file : BehaviorStimulusFile
        Stimulus pickle file to parse.

    Returns
    -------
    stimulus_name : str
        Name of the image stimulus from the image file path set shown to
        the mouse.
    """
    try:
        stimulus_name = Path(
            stim_file["items"]["behavior"]["images"]["image_set"]
        ).stem.split(".")[0]
    except KeyError:
        # if we can't find the images key in the stimuli, check for the
        # name ``grating`` as the stimulus. If not add generic
        # ``behavior``.
        if "grating" in stim_file["items"]["behavior"]["stimuli"].keys():
            stimulus_name = "grating"
        else:
            stimulus_name = "behavior"
    return stimulus_name
