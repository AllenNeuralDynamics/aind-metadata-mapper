""" Constants for the naming utils of metadata mapper """

import re

INT_NULL = -99

default_stimulus_renames = {
    "": "spontaneous",
    "natural_movie_1": "natural_movie_one",
    "natural_movie_3": "natural_movie_three",
    "Natural Images": "natural_scenes",
    "flash_250ms": "flashes",
    "gabor_20_deg_250ms": "gabors",
    "drifting_gratings": "drifting_gratings",
    "static_gratings": "static_gratings",
    "contrast_response": "drifting_gratings_contrast",
    "Natural_Images_Shuffled": "natural_scenes_shuffled",
    "Natural_Images_Sequential": "natural_scenes_sequential",
    "natural_movie_1_more_repeats": "natural_movie_one",
    "natural_movie_shuffled": "natural_movie_one_shuffled",
    "motion_stimulus": "dot_motion",
    "drifting_gratings_more_repeats": "drifting_gratings_75_repeats",
    "signal_noise_test_0_200_repeats": "test_movie_one",
    "signal_noise_test_0": "test_movie_one",
    "signal_noise_test_1": "test_movie_two",
    "signal_noise_session_1": "dense_movie_one",
    "signal_noise_session_2": "dense_movie_two",
    "signal_noise_session_3": "dense_movie_three",
    "signal_noise_session_4": "dense_movie_four",
    "signal_noise_session_5": "dense_movie_five",
    "signal_noise_session_6": "dense_movie_six",
}


default_column_renames = {
    "Contrast": "contrast",
    "Ori": "orientation",
    "Oris": "orientation",
    "SF": "spatial_frequency",
    "TF": "temporal_frequency",
    "Size": "size",
    "Phase": "phase",
    "Color": "color",
    "Image": "frame",
    "Pos_x": "x_position",
    "Pos_y": "y_position",
}


GABOR_DIAMETER_RE = re.compile(
    r"gabor_(\d*\.{0,1}\d*)_{0,1}deg(?:_\d+ms){0,1}"
)

GENERIC_MOVIE_RE = re.compile(
    r"natural_movie_"
    + r"(?P<number>\d+|one|two|three|four|five|six|seven|eight|nine)"
    + r"(_shuffled){0,1}(_more_repeats){0,1}"
)
DIGIT_NAMES = {
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
}
SHUFFLED_MOVIE_RE = re.compile(r"natural_movie_shuffled")
NUMERAL_RE = re.compile(r"(?P<number>\d+)")
