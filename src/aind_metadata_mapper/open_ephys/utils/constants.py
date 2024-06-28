""" Constants for the naming utils of metadata mapper """

import re

INT_NULL = -99
DEFAULT_OPTO_CONDITIONS = {
    "0": {
        "duration": 0.01,
        "name": "1Hz_10ms",
        "condition": "10 ms pulse at 1 Hz",
    },
    "1": {
        "duration": 0.002,
        "name": "1Hz_2ms",
        "condition": "2 ms pulse at 1 Hz",
    },
    "2": {
        "duration": 1.0,
        "name": "5Hz_2ms",
        "condition": "2 ms pulses at 5 Hz",
    },
    "3": {
        "duration": 1.0,
        "name": "10Hz_2ms",
        "condition": "2 ms pulses at 10 Hz",
    },
    "4": {
        "duration": 1.0,
        "name": "20Hz_2ms",
        "condition": "2 ms pulses at 20 Hz",
    },
    "5": {
        "duration": 1.0,
        "name": "30Hz_2ms",
        "condition": "2 ms pulses at 30 Hz",
    },
    "6": {
        "duration": 1.0,
        "name": "40Hz_2ms",
        "condition": "2 ms pulses at 40 Hz",
    },
    "7": {
        "duration": 1.0,
        "name": "50Hz_2ms",
        "condition": "2 ms pulses at 50 Hz",
    },
    "8": {
        "duration": 1.0,
        "name": "60Hz_2ms",
        "condition": "2 ms pulses at 60 Hz",
    },
    "9": {
        "duration": 1.0,
        "name": "80Hz_2ms",
        "condition": "2 ms pulses at 80 Hz",
    },
    "10": {
        "duration": 1.0,
        "name": "square_1s",
        "condition": "1 second square pulse: continuously on for 1s",
    },
    "11": {"duration": 1.0, "name": "cosine_1s", "condition": "cosine pulse"},
}

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
    "SF": "spatial_frequency",
    "TF": "temporal_frequency",
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
