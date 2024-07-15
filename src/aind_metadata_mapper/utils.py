"""Utilities file"""

import logging

from aind_data_schema.base import AindModel
from pydantic import ValidationError


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
