"""Utilities file"""

import logging

from aind_data_schema.base import AindModel
from pydantic import ValidationError
from datetime import datetime
import requests
import json


def setup_logger(logfile_path):
    """initializes the logger for the run script"""

    # set up logger with given file path
    log_file_name = logfile_path + "/log_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log"
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # create file handler which logs even debug messages
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(logging.DEBUG)

    # create console handler, can set the level to info or warning if desired
    # You can remove the console handler if you don't want to see these messages in the
    # notebook.
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)


def download_subject_file(subj_id: str):
    """Download the procedure file for a subject."""

    request = requests.get(
        f"http://aind-metadata-service/subject/{subj_id}"
    )

    if request.status_code == 404:
        logging.error(f"{subj_id} model not found")
        return None

    try:
        item = request.json()
    except json.JSONDecodeError:
        logging.error(f"Error decoding json for {subj_id}: {request.text}")
        return None

    if item["message"] == "Valid Model.":
        return item["data"]
    elif "Validation Errors:" in item["message"]:
        logging.warning(f"Validation errors for {subj_id}")
        return item["data"]
    return None


def construct_new_model(
    model_inputs: dict, model_type: AindModel, allow_validation_errors=False
):
    """
    Validate a model,
    if it fails and validation error flag is on, construct a model
    """

    try:
        return model_type.model_validate(model_inputs)
    except ValidationError as e:
        logging.error(f"Validation error in {type(model_type)}: {e}")
        logging.error(f"allow validation errors: {allow_validation_errors}")
        if allow_validation_errors:
            logging.error(f"Attempting to construct model {model_inputs}")
            m = model_type.model_construct(**model_inputs)
            logging.error(f"Model constructed: {m}")
            return m
        else:
            raise e
