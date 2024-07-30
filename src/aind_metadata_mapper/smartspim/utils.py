"""
Utility functions for SmartSPIM
"""

import json
import os
from datetime import datetime
from typing import List, Optional

from aind_data_schema.components import tile
from aind_data_schema.components.coordinates import AnatomicalDirection
from aind_data_schema_models.units import PowerUnit, SizeUnit


def read_json_as_dict(filepath: str) -> dict:
    """
    Reads a json as dictionary.
    Parameters
    ------------------------
    filepath: PathLike
        Path where the json is located.
    Returns
    ------------------------
    dict:
        Dictionary with the data the json has.
    """

    dictionary = {}

    if os.path.exists(filepath):
        try:
            with open(filepath) as json_file:
                dictionary = json.load(json_file)

        except UnicodeDecodeError:
            # print("Error reading json with utf-8, trying different approach")
            # This might lose data, verify with Jeff the json encoding
            with open(filepath, "rb") as json_file:
                data = json_file.read()
                data_str = data.decode("utf-8", errors="ignore")
                dictionary = json.loads(data_str)

            # print(f"Reading {filepath} forced: {dictionary}")

    return dictionary


def get_anatomical_direction(anatomical_direction: str) -> AnatomicalDirection:
    """
    This function returns the correct anatomical
    direction defined in the aind_data_schema.

    Parameters
    ----------
    anatomical_direction: str
        String defining the anatomical direction
        of the data

    Returns
    -------
    AnatomicalDirection: class::Enum
        Corresponding enum defined in the anatomical
        direction class
    """

    anatomical_direction = anatomical_direction.lower()

    # Note: I could have done str.capitalize and then get
    # the anatomical direction directly from the class
    # but we have multiple versions now and I want to make this
    # robust
    if anatomical_direction == "left_to_right":
        anatomical_direction = AnatomicalDirection.LR

    elif anatomical_direction == "right_to_left":
        anatomical_direction = AnatomicalDirection.RL

    elif anatomical_direction == "anterior_to_posterior":
        anatomical_direction = AnatomicalDirection.AP

    elif anatomical_direction == "posterior_to_anterior":
        anatomical_direction = AnatomicalDirection.PA

    elif anatomical_direction == "inferior_to_superior":
        anatomical_direction = AnatomicalDirection.IS

    elif anatomical_direction == "superior_to_inferior":
        anatomical_direction = AnatomicalDirection.SI

    return anatomical_direction


def make_acq_tiles(metadata_dict: dict, filter_mapping: dict):
    """
    Makes metadata for the acquired tiles of
    the dataset

    Parameters
    -----------
    metadata_dict: dict
        Dictionary with the acquisition metadata
        coming from the microscope

    filter_mapping: dict
        Dictionary with the channel names

    Returns
    -----------
    List[tile.Translation3dTransform]
        List with the metadata for the tiles
    """

    channels = {}

    # List where the metadata of the acquired
    # tiles is stored
    tile_acquisitions = []

    filter_wheel_idx = 0
    for wavelength, value in metadata_dict["wavelength_config"].items():
        channel = tile.Channel(
            channel_name=str(wavelength),
            light_source_name=str(wavelength),
            filter_names=[""],  # We don't have filter names at the moment
            detector_name="",  # We don't have detector names at the moment
            additional_device_names=[],
            # Excitation wavelengths
            excitation_wavelength=int(wavelength),
            excitation_wavelength_unit=SizeUnit.NM,
            # Excitation power
            excitation_power=value[
                "power_left"
            ],  # [value["power_left"], value["power_right"]]
            excitation_power_unit=PowerUnit.PERCENT,
            filter_wheel_index=filter_wheel_idx,
        )
        filter_wheel_idx += 1

        channels[wavelength] = channel

    # Scale metadata
    session_config = metadata_dict.get("session_config")

    x_res = y_res = session_config.get("µm/pix")
    z_res = session_config.get("z_step_um")

    # utf-8 error with micron symbol
    if x_res is None:
        x_res = y_res = session_config.get("m/pix")
        if x_res is None:
            raise KeyError("Failed getting the x and y resolution from metadata.json")

    if z_res is None:
        z_res = session_config.get("Z step (m)")

        if z_res is None:
            raise KeyError("Failed to get the Z step in microns")

    x_res = float(x_res)
    y_res = float(y_res)
    z_res = float(z_res)

    scale = tile.Scale3dTransform(
        scale=[
            x_res,  # X res
            y_res,  # Y res
            z_res,  # Z res
        ]
    )

    for tile_key, tile_info in metadata_dict["tile_config"].items():
        tile_info_x = tile_info.get("x")
        tile_info_y = tile_info.get("y")
        tile_info_z = tile_info.get("z")

        # For some reason, Jeff changed the lower case to upper case
        if tile_info_x is None:
            tile_info_x = tile_info.get("X")

        if tile_info_y is None:
            tile_info_y = tile_info.get("Y")

        if tile_info_z is None:
            tile_info_z = tile_info.get("Z")

        tile_info_x = float(tile_info_x)
        tile_info_y = float(tile_info_y)
        tile_info_z = float(tile_info_z)

        tile_transform = tile.Translation3dTransform(
            translation=[
                int(tile_info_x) / 10,
                int(tile_info_y) / 10,
                int(tile_info_z) / 10,
            ]
        )

        channel = channels[tile_info["Laser"]]
        exaltation_wave = int(tile_info["Laser"])
        emission_wave = filter_mapping[exaltation_wave]

        tile_acquisition = tile.AcquisitionTile(
            channel=channel,
            notes=("\nLaser power is in percentage of total, it needs calibration"),
            coordinate_transformations=[tile_transform, scale],
            file_name=f"Ex_{exaltation_wave}_Em_{emission_wave}/"
            f"{tile_info_x}/{tile_info_x}_{tile_info_y}/",
        )

        tile_acquisitions.append(tile_acquisition)

    return tile_acquisitions


def digest_asi_line(line: str) -> Optional[datetime]:
    """
    Scrape a datetime from a non-empty line, otherwise return None

    Parameters
    -----------
    line: str
        Line from the ASI file

    Returns
    -----------
    datetime
        A date that could be parsed from a string
    """

    if line.isspace():
        return None
    else:
        mdy, hms, ampm = line.split()[0:3]

    mdy = [int(i) for i in mdy.split(b"/")]
    ymd = [mdy[i] for i in [2, 0, 1]]

    hms = [int(i) for i in hms.split(b":")]
    if ampm == b"PM":
        hms[0] += 12
        if hms[0] == 24:
            hms[0] = 0

    ymdhms = ymd + hms

    dtime = datetime(*ymdhms)
    return dtime


def get_session_end(asi_file: os.PathLike) -> datetime:
    """
    Work backward from the last line until there is a timestamp

    Parameters
    ------------
    asi_file: PathLike
        Path where the ASI metadata file is
        located

    Returns
    ------------
    Date when the session ended
    """

    with open(asi_file, "rb") as file:
        asi_mdata = file.readlines()

    idx = -1
    result = None
    while result is None:
        result = digest_asi_line(asi_mdata[idx])
        idx -= 1

    return result


def get_excitation_emission_waves(channels: List) -> dict:
    """
    Gets the excitation and emission waves for
    the existing channels within a dataset

    Parameters
    ------------
    channels: List[str]
        List with the channels.
        They must contain the emmision
        wavelenght in the name

    Returns
    ------------
    dict
        Dictionary with the excitation
        and emission waves
    """
    excitation_emission_channels = {}

    for channel in channels:
        channel = channel.replace("Em_", "").replace("Ex_", "")
        splitted = channel.split("_")
        excitation_emission_channels[int(splitted[0])] = int(splitted[1])

    return excitation_emission_channels
