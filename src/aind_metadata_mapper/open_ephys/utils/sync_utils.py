""" Functions for working with sync files. """

import datetime
import logging
from typing import Optional, Sequence, Union

import h5py
import numpy as np
import scipy.spatial.distance as distance

import aind_metadata_mapper.open_ephys.utils.pkl_utils as pkl

logger = logging.getLogger(__name__)


def load_sync(path):
    """
    Loads an hdf5 sync dataset.

    Parameters
    ----------
    path : str
        Path to hdf5 file.

    Returns
    -------
    dfile : h5py.File
        Loaded hdf5 file.
    """
    dfile = h5py.File(path, "r")
    return dfile


def get_meta_data(sync_file):
    """
    Gets the meta data from the sync file.

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    meta_data : dict
        Meta data from the sync file.
    """
    meta_data = eval(sync_file["meta"][()])
    return meta_data


def get_line_labels(sync_file):
    """
    Gets the line labels from the sync file.

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    line_labels : list
        Line labels from the sync file.
    """
    meta_data = get_meta_data(sync_file)
    line_labels = meta_data["line_labels"]
    return line_labels


def get_times(sync_file):
    """
    Gets the times from the sync file.

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    times : np.ndarray
        Times from the sync file.
    """
    times = process_times(sync_file)
    return times


def get_start_time(sync_file) -> datetime.datetime:
    """
    Gets the start timefrom the sync file.

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    meta_data : dict
        Meta data from the sync file.
    """
    meta_data = get_meta_data(sync_file)
    return datetime.datetime.fromisoformat(meta_data["start_time"])


def get_total_seconds(sync_file) -> float:
    """
    Gets the overall length from the sync file.

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    data: float
        Total seconds.
    """
    meta_data = get_meta_data(sync_file)
    return meta_data["total_samples"] / get_sample_freq(meta_data)


def get_stop_time(sync_file) -> datetime.datetime:
    """
    Gets the stop time from the sync file.

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    data: datetime.datetime
        Stop time.
    """
    start_time = get_start_time(sync_file)
    total_seconds = get_total_seconds(sync_file)
    return start_time + datetime.timedelta(seconds=total_seconds)


def extract_led_times(sync_file, keys="", fallback_line=18):
    """
    Extracts the LED times from the sync file.
    Rising or Falling

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    led_times : np.ndarray
        LED times.
    """

    try:
        led_times = get_edges(
            sync_file=sync_file, kind="rising", keys=keys, units="seconds"
        )
    except KeyError:
        led_times = get_rising_edges(sync_file, fallback_line, units="seconds")

    return led_times


def process_times(sync_file):
    """
    Processes the times from the sync file.
    Checks for rollover times

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    times : np.ndarray
        Times from the sync file.

    """
    times = sync_file["data"][()][:, 0:1].astype(np.int64)

    intervals = np.ediff1d(times, to_begin=0)
    rollovers = np.where(intervals < 0)[0]

    for i in rollovers:
        times[i:] += 4294967296

    return times


def get_ophys_stimulus_timestamps(sync, pkl):
    """Obtain visual behavior stimuli timing information from a sync *.h5 file.

    Parameters
    ----------
    sync_path : Union[str, Path]
        The path to a sync *.h5 file that contains global timing information
        about multiple data streams (e.g. behavior, ophys, eye_tracking)
        during a session.

    Returns
    -------
    np.ndarray
        Timestamps (in seconds) for presented stimulus frames during a
        behavior + ophys session.
    """
    stimulus_timestamps, _ = get_clipped_stim_timestamps(sync, pkl)
    return stimulus_timestamps


def get_stim_data_length(filename: str) -> int:
    """Get stimulus data length from .pkl file.

    Parameters
    ----------
    filename : str
        Path of stimulus data .pkl file.

    Returns
    -------
    int
        Stimulus data length.
    """
    stim_data = pkl.load_pkl(filename)

    # A subset of stimulus .pkl files do not have the "vsynccount" field.
    # MPE *won't* be backfilling the "vsynccount" field for these .pkl files.
    # So the least worst option is to recalculate the vsync_count.
    try:
        vsync_count = stim_data["vsynccount"]
    except KeyError:
        vsync_count = len(stim_data["items"]["behavior"]["intervalsms"]) + 1

    return vsync_count


def get_behavior_stim_timestamps(sync):
    """
    Get stimulus timestamps from the behavior stream in the sync file.
    Checks various line labels

    Parameters
    ----------
    sync : h5py.File
        Sync file.

    Returns
    -------
    times : np.ndarray
        Timestamps.
    """
    try:
        stim_key = "vsync_stim"
        times = get_falling_edges(sync, stim_key, units="seconds")
        return times
    except ValueError:
        stim_key = "stim_vsync"
        times = get_falling_edges(sync, stim_key, units="seconds")
        return times
    except Exception:
        raise ValueError("No stimulus stream found in sync file")


def get_clipped_stim_timestamps(sync, pkl_path):
    """
    Get stimulus timestamps from the behavior stream in the sync file.
    Checks various line labels
    Clips based on length

    Parameters
    ----------
    sync : h5py.File
        Sync file.
    pkl_path : str
        Path to pkl file

    Returns
    -------
    timestamps : np.ndarray
        Timestamps.
    delta: int
        Difference in length
    """

    timestamps = get_behavior_stim_timestamps(sync)
    stim_data_length = get_stim_data_length(pkl_path)

    delta = 0
    logger.debug(sync)
    if stim_data_length is not None and stim_data_length < len(timestamps):
        try:
            stim_key = "vsync_stim"
            rising = get_rising_edges(sync, stim_key, units="seconds")
        except ValueError:
            stim_key = "stim_vsync"
            rising = get_rising_edges(sync, stim_key, units="seconds")
        except Exception:
            raise ValueError("No stimulus stream found in sync file")

        # Some versions of camstim caused a spike when the DAQ is first
        # initialized. Remove it.
        if rising[1] - rising[0] > 0.2:
            logger.debug(
                "Initial DAQ spike detected from stimulus, " "removing it"
            )
            timestamps = timestamps[1:]

        delta = len(timestamps) - stim_data_length
        if delta != 0:
            logger.debug(
                "Stim data of length %s has timestamps of " "length %s",
                stim_data_length,
                len(timestamps),
            )
    elif stim_data_length is None:
        logger.debug("No data length provided for stim stream")
    return timestamps, delta


def line_to_bit(sync_file, line):
    """
    Returns the bit for a specified line.  Either line name and number is
        accepted.

    Parameters
    ----------
    line : str
        Line name for which to return corresponding bit.

    returns
    -------
    bit : int
        Bit for the line.

    """
    line_labels = get_line_labels(sync_file)

    if type(line) is int:
        return line
    elif type(line) is str:
        return line_labels.index(line)
    else:
        raise TypeError("Incorrect line type.  Try a str or int.")


def get_edges(
    sync_file: h5py.File,
    kind: str,
    keys: Union[str, Sequence[str]],
    units: str = "seconds",
    permissive: bool = False,
) -> Optional[np.ndarray]:
    """
    Utility function for extracting edge times from a line

    Parameters
    ----------
    kind : One of "rising", "falling", or "all". Should this method return
        timestamps for rising, falling or both edges on the appropriate
        line
    keys : These will be checked in sequence. Timestamps will be returned
        for the first which is present in the line labels
    units : one of "seconds", "samples", or "indices". The returned
        "time"stamps will be given in these units.
    raise_missing : If True and no matching line is found, a KeyError will
        be raised

    Returns
    -------
    An array of edge times. If raise_missing is False and none of the keys
        were found, returns None.

    Raises
    ------
    KeyError : none of the provided keys were found among this dataset's
        line labels

    """

    if isinstance(keys, str):
        keys = [keys]

    logger.debug(keys)

    for line in keys:
        try:
            if kind == "falling":
                return get_falling_edges(sync_file, line, units)
            elif kind == "rising":
                return get_rising_edges(sync_file, line, units)
            elif kind == "all":
                return np.sort(
                    np.concatenate(
                        [
                            get_edges(sync_file, "rising", keys, units),
                            get_edges(sync_file, "falling", keys, units),
                        ]
                    )
                )
        except ValueError:
            continue

    if not permissive:
        raise KeyError(
            f"none of {keys} were found in this dataset's line labels"
        )


def get_bit_changes(sync_file, bit):
    """
    Returns the first derivative of a specific bit.
        Data points are 1 on rising edges and 255 on falling edges.

    Parameters
    ----------
    bit : int
        Bit for which to return changes.

    """
    bit_array = get_sync_file_bit(sync_file, bit)
    return np.ediff1d(bit_array, to_begin=0)


def get_all_bits(sync_file):
    """
    Returns all counter values.

    Parameters
    ----------
    sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    data: np.ndarray
        All counter values.
    """
    return sync_file["data"][()][:, -1]


def get_sync_file_bit(sync_file, bit):
    """
    Returns a specific bit from the sync file.

    Parameters
    ----------
    bit : int
        Bit to extract.
    Sync_file : h5py.File
        Loaded hdf5 file.

    Returns
    -------
    data: np.ndarray
        Bit values.
    """

    return get_bit(get_all_bits(sync_file), bit)


def get_bit(uint_array, bit):
    """
    Returns a bool array for a specific bit in a uint ndarray.

    Parameters
    ----------
    uint_array : (numpy.ndarray)
        The array to extract bits from.
    bit : (int)
        The bit to extract.

    """
    return np.bitwise_and(uint_array, 2**bit).astype(bool).astype(np.uint8)


def get_sample_freq(meta_data):
    """
    Returns the sample frequency from the meta data.

    Parameters
    ----------
    meta_data : dict
        Meta data from the sync file.

    Returns
    -------
    data: float
        Sample frequency.
    """

    try:
        return float(meta_data["ni_daq"]["sample_freq"])
    except KeyError:
        return float(meta_data["ni_daq"]["counter_output_freq"])


def get_all_times(sync_file, meta_data, units="samples"):
    """
    Returns all counter values.

    Parameters
    ----------
    units : str
        Return times in 'samples' or 'seconds'

    """
    if meta_data["ni_daq"]["counter_bits"] == 32:
        times = sync_file["data"][()][:, 0]
    else:
        times = 0
    units = units.lower()
    if units == "samples":
        return times
    elif units in ["seconds", "sec", "secs"]:
        freq = get_sample_freq(meta_data)
        return times / freq
    else:
        raise ValueError("Only 'samples' or 'seconds' are valid units.")


def get_falling_edges(sync_file, line, units="samples"):
    """
    Returns the counter values for the falling edges for a specific bit
        or line.

    Parameters
    ----------
    line : str
        Line for which to return edges.

    """
    meta_data = get_meta_data(sync_file)
    bit = line_to_bit(sync_file, line)
    changes = get_bit_changes(sync_file, bit)
    return get_all_times(sync_file, meta_data, units)[np.where(changes == 255)]


def get_rising_edges(sync_file, line, units="samples"):
    """
    Returns the counter values for the rizing edges for a specific bit or
        line.

    Parameters
    ----------
    line : str
        Line for which to return edges.

    """
    meta_data = get_meta_data(sync_file)
    bit = line_to_bit(sync_file, line)
    changes = get_bit_changes(sync_file, bit)
    return get_all_times(sync_file, meta_data, units)[np.where(changes == 1)]


def trimmed_stats(data, pctiles=(10, 90)):
    """
    Returns the mean and standard deviation of the data after trimming the
        data at the specified percentiles.

    Parameters
    ----------
    data : np.ndarray
        Data to trim.
    pctiles : tuple
        Percentiles at which to trim the data.

    Returns
    -------
    mean : float
        Mean of the trimmed data.
    std : float
        Standard deviation of the trimmed data.
    """
    low = np.percentile(data, pctiles[0])
    high = np.percentile(data, pctiles[1])

    trimmed = data[np.logical_and(data <= high, data >= low)]

    return np.mean(trimmed), np.std(trimmed)


def estimate_frame_duration(pd_times, cycle=60):
    """
    Estimates the frame duration from the photodiode times.

    Parameters
    ----------

    pd_times : np.ndarray
        Photodiode times.
    cycle : int
        Number of frames per cycle.

    Returns
    -------
    frame_duration : float
        Estimated frame duration.
    """
    return trimmed_stats(np.diff(pd_times))[0] / cycle


def allocate_by_vsync(
    vs_diff, index, starts, ends, frame_duration, irregularity, cycle
):
    """
    Allocates frame times based on the vsync signal.

    Parameters
    ----------
    vs_diff : np.ndarray
        Difference between vsync times.
    index : int
        Index of the current vsync.
    starts : np.ndarray
        Start times of the frames.
    ends : np.ndarray
        End times of the frames.
    frame_duration : float
        Duration of the frame.
    irregularity : int
        Irregularity in the frame times.
    cycle : int
        Number of frames per cycle.

    Returns
    -------
    starts : np.ndarray
        Start times of the frames.
    ends : np.ndarray
        End times of the frames.
    """
    current_vs_diff = vs_diff[index * cycle : (index + 1) * cycle]
    sign = np.sign(irregularity)

    if sign > 0:
        vs_ind = np.argmax(current_vs_diff)
    elif sign < 0:
        vs_ind = np.argmin(current_vs_diff)

    ends[vs_ind:] += sign * frame_duration
    starts[vs_ind + 1 :] += sign * frame_duration

    return starts, ends


def trim_border_pulses(
    pd_times, vs_times, frame_interval=1 / 60, num_frames=5
):
    """
    Trims pulses near borders of the photodiode signal.

    Parameters
    ----------
    pd_times : np.ndarray
        Photodiode times.
    vs_times : np.ndarray
        Vsync times.
    frame_interval : float
        Interval between frames.
    num_frames : int
        Number of frames.

    Returns
    -------
    pd_times : np.ndarray
        Trimmed photodiode times.
    """
    pd_times = np.array(pd_times)
    return pd_times[
        np.logical_and(
            pd_times >= vs_times[0],
            pd_times <= vs_times[-1] + num_frames * frame_interval,
        )
    ]


def correct_on_off_effects(pd_times):
    """

    Notes
    -----
    This cannot (without additional info) determine whether an assymmetric
    offset is odd-long or even-long.
    """

    pd_diff = np.diff(pd_times)
    odd_diff_mean, odd_diff_std = trimmed_stats(pd_diff[1::2])
    even_diff_mean, even_diff_std = trimmed_stats(pd_diff[0::2])

    half_diff = np.diff(pd_times[0::2])
    full_period_mean, full_period_std = trimmed_stats(half_diff)
    half_period_mean = full_period_mean / 2

    odd_offset = odd_diff_mean - half_period_mean
    even_offset = even_diff_mean - half_period_mean

    pd_times[::2] -= odd_offset / 2
    pd_times[1::2] -= even_offset / 2

    return pd_times


def trim_discontiguous_vsyncs(vs_times, photodiode_cycle=60):
    """
    Trims discontiguous vsyncs from the photodiode signal.

    Parameters
    ----------
    vs_times : np.ndarray
        Vsync times.
    photodiode_cycle : int
        Number of frames per cycle.

    Returns
    -------
    vs_times : np.ndarray
        Trimmed vsync times.
    """
    vs_times = np.array(vs_times)

    breaks = np.where(np.diff(vs_times) > (1 / photodiode_cycle) * 100)[0]

    if len(breaks) > 0:
        chunk_sizes = np.diff(
            np.concatenate(
                (
                    np.array(
                        [
                            0,
                        ]
                    ),
                    breaks,
                    np.array(
                        [
                            len(vs_times),
                        ]
                    ),
                )
            )
        )
        largest_chunk = np.argmax(chunk_sizes)

        if largest_chunk == 0:
            return vs_times[: np.min(breaks + 1)]
        elif largest_chunk == len(breaks):
            return vs_times[np.max(breaks + 1) :]
        else:
            return vs_times[breaks[largest_chunk - 1] : breaks[largest_chunk]]
    else:
        return vs_times


def assign_to_last(starts, ends, frame_duration, irregularity):
    """
    Assigns the irregularity to the last frame.

    Parameters
    ----------
    starts : np.ndarray
        Start times of the frames.
    ends : np.ndarray
        End times of the frames.
    frame_duration : float
        Duration of the frame.
    irregularity : int
        Irregularity in the frame times.

    Returns
    -------
    starts : np.ndarray
        Start times of the frames.
    ends : np.ndarray
        Modified end times of the frames.
    """
    ends[-1] += frame_duration * np.sign(irregularity)
    return starts, ends


def remove_zero_frames(frame_times):
    """
    Removes zero delta frames from the frame times.

    Parameters
    ----------
    frame_times : np.ndarray
        Frame times.

    Returns
    -------
    t : np.ndarray
        Modified frame times.
    """
    deltas = np.diff(frame_times)

    small_deltas = np.where(deltas < 0.01)[0]
    big_deltas = np.where((deltas > 0.018) * (deltas < 0.1))[0]

    def find_match(big_deltas, value):
        """
        Finds max match for the value in the big deltas.

        Parameters
        ----------
        big_deltas : np.ndarray
            Big deltas.
        value : float
            Value to match.

        Returns
        -------
        float
            Matched value.
        """

        try:
            return (
                big_deltas[np.max(np.where((big_deltas < value))[0])] - value
            )
        except ValueError:
            return None

    paired_deltas = [find_match(big_deltas, A) for A in small_deltas]

    ft = np.copy(deltas)

    for idx, d in enumerate(small_deltas):
        if paired_deltas[idx] is not None:
            if paired_deltas[idx] > -100:
                ft[d + paired_deltas[idx]] = np.median(deltas)
                ft[d] = np.median(deltas)

    t = np.concatenate(
        ([np.min(frame_times)], np.cumsum(ft) + np.min(frame_times))
    )

    return t


def compute_frame_times(
    photodiode_times,
    frame_duration,
    num_frames,
    cycle,
    irregular_interval_policy=assign_to_last,
):
    """
    Computes the frame times from the photodiode times.

    Parameters
    ----------
    photodiode_times : np.ndarray
        Photodiode times.
    frame_duration : float
        Duration of the frame.
    num_frames : int
        Number of frames.
    cycle : int
        Number of frames per cycle.
    irregular_interval_policy : function
        Policy for handling irregular intervals.

    Returns
    -------
    indices : np.ndarray
        Indices of the frames.
    starts : np.ndarray
        Start times of the frames.
    ends : np.ndarray
        End times of the frames.
    """
    indices = np.arange(num_frames)
    starts = np.zeros(num_frames, dtype=float)
    ends = np.zeros(num_frames, dtype=float)

    num_intervals = len(photodiode_times) - 1
    for start_index, (start_time, end_time) in enumerate(
        zip(photodiode_times[:-1], photodiode_times[1:])
    ):
        interval_duration = end_time - start_time
        irregularity = (
            int(np.around((interval_duration) / frame_duration)) - cycle
        )

        local_frame_duration = interval_duration / (cycle + irregularity)
        durations = (
            np.zeros(cycle + (start_index == num_intervals - 1))
            + local_frame_duration
        )

        current_ends = np.cumsum(durations) + start_time
        current_starts = current_ends - durations

        while irregularity != 0:
            current_starts, current_ends = irregular_interval_policy(
                start_index,
                current_starts,
                current_ends,
                local_frame_duration,
                irregularity,
                cycle,
            )
            irregularity += -1 * np.sign(irregularity)

        early_frame = start_index * cycle
        late_frame = (start_index + 1) * cycle + (
            start_index == num_intervals - 1
        )

        remaining = starts[early_frame:late_frame].size
        starts[early_frame:late_frame] = current_starts[:remaining]
        ends[early_frame:late_frame] = current_ends[:remaining]

    return indices, starts, ends


def separate_vsyncs_and_photodiode_times(
    vs_times, pd_times, photodiode_cycle=60
):
    """
    Separates the vsyncs and photodiode times.

    Parameters
    ----------
    vs_times : np.ndarray
        Vsync times.
    pd_times : np.ndarray
        Photodiode times.

    Returns
    -------
    vs_times_out : np.ndarray
        Vsync times.
    pd_times_out : np.ndarray
        Photodiode times.
    """
    vs_times = np.array(vs_times)
    pd_times = np.array(pd_times)

    breaks = np.where(np.diff(vs_times) > (1 / photodiode_cycle) * 100)[0]

    shift = 2.0
    break_times = [-shift]
    break_times.extend(vs_times[breaks].tolist())
    break_times.extend([np.inf])

    vs_times_out = []
    pd_times_out = []

    for indx, b in enumerate(break_times[:-1]):
        pd_in_range = np.where(
            (pd_times > break_times[indx] + shift)
            * (pd_times <= break_times[indx + 1] + shift)
        )[0]
        vs_in_range = np.where(
            (vs_times > break_times[indx])
            * (vs_times <= break_times[indx + 1])
        )[0]

        vs_times_out.append(vs_times[vs_in_range])
        pd_times_out.append(pd_times[pd_in_range])

    return vs_times_out, pd_times_out


def flag_unexpected_edges(pd_times, ndevs=10):
    """
    Flags unexpected edges in the photodiode times.

    Parameters
    ----------
    pd_times : np.ndarray
        Photodiode times.
    ndevs : int
        Number of standard deviations.

    Returns
    -------
    expected_duration_mask : np.ndarray
        Mask for expected durations.
    """
    pd_diff = np.diff(pd_times)
    diff_mean, diff_std = trimmed_stats(pd_diff)

    expected_duration_mask = np.ones(pd_diff.size)
    expected_duration_mask[
        np.logical_or(
            pd_diff < diff_mean - ndevs * diff_std,
            pd_diff > diff_mean + ndevs * diff_std,
        )
    ] = 0
    expected_duration_mask[1:] = np.logical_and(
        expected_duration_mask[:-1], expected_duration_mask[1:]
    )
    expected_duration_mask = np.concatenate(
        [expected_duration_mask, [expected_duration_mask[-1]]]
    )

    return expected_duration_mask


def fix_unexpected_edges(pd_times, ndevs=10, cycle=60, max_frame_offset=4):
    """
    Fixes unexpected edges in the photodiode times.

    Parameters
    ----------
    pd_times : np.ndarray
        Photodiode times.
    ndevs : int
        Number of standard deviations.
    cycle : int
        Number of frames per cycle.
    max_frame_offset : int
        Maximum frame offset.

    Returns
    -------
    output_edges : np.ndarray
        Output edges.
    """
    pd_times = np.array(pd_times)
    expected_duration_mask = flag_unexpected_edges(pd_times, ndevs=ndevs)
    diff_mean, diff_std = trimmed_stats(np.diff(pd_times))
    frame_interval = diff_mean / cycle

    bad_edges = np.where(expected_duration_mask == 0)[0]
    bad_blocks = np.sort(
        np.unique(
            np.concatenate(
                [
                    [0],
                    np.where(np.diff(bad_edges) > 1)[0] + 1,
                    [len(bad_edges)],
                ]
            )
        )
    )

    output_edges = []
    for low, high in zip(bad_blocks[:-1], bad_blocks[1:]):
        current_bad_edge_indices = bad_edges[low : high - 1]
        current_bad_edges = pd_times[current_bad_edge_indices]
        low_bound = pd_times[current_bad_edge_indices[0]]
        high_bound = pd_times[current_bad_edge_indices[-1] + 1]

        edges_missing = int(np.around((high_bound - low_bound) / diff_mean))
        expected = np.linspace(low_bound, high_bound, edges_missing + 1)

        distances = distance.cdist(
            current_bad_edges[:, None], expected[:, None]
        )
        distances = np.around(distances / frame_interval).astype(int)

        min_offsets = np.amin(distances, axis=0)
        min_offset_indices = np.argmin(distances, axis=0)
        output_edges = np.concatenate(
            [
                output_edges,
                expected[min_offsets > max_frame_offset],
                current_bad_edges[
                    min_offset_indices[min_offsets <= max_frame_offset]
                ],
            ]
        )

    return np.sort(
        np.concatenate([output_edges, pd_times[expected_duration_mask > 0]])
    )
