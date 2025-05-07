""" Utils to process pkl files"""

import pickle

import numpy as np
import pandas as pd


def load_pkl(path):
    """
    Loads a pkl stim file

    Parameters
    ----------
    path : str
        Path to pkl file.

    Returns
    -------
    data : dict
        pkl file.
    """
    data = pd.read_pickle(path)
    return data


def load_img_pkl(pstream):
    """
    Loads a pkl stim file

    Parameters
    ----------
    pstream : str
        image pkl file.

    """
    return pickle.load(pstream, encoding="Latin-1")


def get_stimuli(pkl, is_behavior=False):
    """
    Returns the stimuli from a pkl file

    Parameters
    ----------
    pkl : dict
        pkl file.

    """

    if is_behavior:
        return pkl["items"]["behavior"]["items"]
    return pkl["stimuli"]


def get_fps(pkl):
    """
    Returns the fps from a pkl file

    Parameters
    ----------
    pkl : dict
        pkl file.

    Returns
    -------
    data: int
        fps.

    """
    if not pkl.get("fps"):
        fps = round(
            1 / np.mean(pkl["items"]["behavior"]["intervalsms"]) * 0.001, 2
        )
    else:
        fps = pkl["fps"]
    return fps


def get_pre_blank_sec(pkl):
    """
    Returns the pre_blank_sec from a pkl file

    Parameters
    ----------

    pkl : dict
        pkl file.

    Returns
    -------
    data: int
        pre_blank_sec.

    """
    return pkl["pre_blank_sec"]


def angular_wheel_velocity(pkl):
    """
    Returns the wheel velocity from a pkl file

    Parameters
    ----------
    pkl : dict
        pkl file.

    Returns
    -------
    data: int
        fps * wheel rotation speed

    """
    return get_fps(pkl) * get_angular_wheel_rotation(pkl)


def get_stage(pkl):
    """
    Returns the stage from a pkl file

    Parameters
    ----------
    pkl : dict
        pkl file.

    Returns
    -------
    data: str
        stage name
    """
    if "stage" in pkl:
        return pkl["stage"]
    elif "items" in pkl:
        return pkl["items"]["behavior"]["cl_params"]["stage"]


def get_angular_wheel_rotation(pkl):
    """
    Returns the wheel rotation from a pkl file

    Parameters
    ----------
    pkl : dict
        pkl file.

    Returns
    -------
    data: int
        wheel rotation speed

    """
    return get_running_array(pkl, "dx")


def vsig(pkl):
    """
    Returns the vsig from a pkl file

    Parameters
    ----------
    pkl : dict
        pkl file.

    Returns
    -------
    data: int
        vsig

    """
    return get_running_array(pkl, "vsig")


def vin(pkl):
    """
    Returns the voltage in from a pkl file

    Parameters
    ----------

    pkl : dict
        pkl file.

    Returns
    -------
    data: vin
        voltage in

    """
    return get_running_array(pkl, "vin")


def get_running_array(pkl, key):
    """
    Returns an running array from a pkl file

    Parameters
    ----------
    pkl : dict
        pkl file.
    key : str
        key to extract from pkl file.

    Returns
    -------
    data: array
        running array

    """
    try:
        result = pkl["items"]["foraging"]["encoders"][0][key]
    except (KeyError, IndexError):
        try:
            result = pkl[key]
        except KeyError:
            raise KeyError(
                f"unable to extract {key} from this stimulus pickle"
            )

    return np.array(result)
