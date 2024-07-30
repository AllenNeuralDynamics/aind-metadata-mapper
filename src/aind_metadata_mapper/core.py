"""Core abstract class that can be used as a template for etl jobs."""

import argparse
import logging
from abc import ABC, abstractmethod
from os import PathLike
from pathlib import Path
from typing import Any, Generic, Optional, TypeVar, Union

from aind_data_schema.base import AindCoreModel
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_settings import BaseSettings

_T = TypeVar("_T", bound=BaseSettings)


class JobResponse(BaseModel):
    """Standard model of a JobResponse."""

    model_config = ConfigDict(extra="forbid")
    status_code: int
    message: Optional[str] = Field(None)
    data: Optional[str] = Field(None)


class GenericEtl(ABC, Generic[_T]):
    """A generic etl class. Child classes will need to create a JobSettings
    object that is json serializable. Child class will also need to implement
    the run_job method, which returns a JobResponse object."""

    def __init__(self, job_settings: _T):
        """
        Class constructor for the GenericEtl class.
        Parameters
        ----------
        job_settings : _T
          Generic type that is bound by the BaseSettings class.
        """
        self.job_settings = job_settings

    @staticmethod
    def _run_validation_check(
        model_instance: AindCoreModel,
    ) -> Optional[ValidationError]:
        """
        Run a validation check on the model_instance.
        Parameters
        ----------
        model_instance : AindCoreModel
          Model to validate.

        Returns
        -------
        Optional[ValidationError]
          None if no validation errors are detected. Else, returns the
          ValidationError object.

        """
        try:
            model_instance.model_validate(model_instance.__dict__)
            logging.debug("No validation errors detected.")
            return None
        except ValidationError as e:
            logging.debug(f"Validation errors detected: {repr(e)}")
            return e

    def _load(
        self, output_model: AindCoreModel, output_directory: Optional[Path]
    ) -> JobResponse:
        """
        Will write to an output directory if an output_directory is not None.
        If output_directory is None, then the model will be returned as json
        in the JobResponse object.
        Parameters
        ----------
        output_model : AindCoreModel
          The final model that has been constructed.
        output_directory : Optional[Path]
          Path to write the model to.

        Returns
        -------
        JobResponse
          The JobResponse object with information about the model. The
          status_codes are:
          200 - No validation errors on the model and written without errors
          406 - There were validation errors on the model
          500 - There were errors writing the model to output_directory

        """
        validation_errors = self._run_validation_check(output_model)
        if validation_errors:
            validation_message = f"Validation errors detected: {repr(validation_errors)}"
            status_code = 406
        else:
            validation_message = "No validation errors detected."
            status_code = 200
        if output_directory is None:
            data = output_model.model_dump_json()
            message = validation_message
        else:
            data = None
            try:
                output_model.write_standard_file(output_directory=output_directory)
                message = f"Write model to {output_directory}\n" + validation_message
            except Exception as e:
                message = (
                    f"Error writing to {output_directory}: {repr(e)}\n"
                    + validation_message
                )
                status_code = 500
        return JobResponse(status_code=status_code, message=message, data=data)

    @abstractmethod
    def run_job(self) -> JobResponse:
        """Abstract method that needs to be implemented by child classes."""


# TODO: Deprecated class
class BaseEtl(ABC):
    """Base etl class. Defines interface for extracting, transforming, and
    loading input sources into a json file saved locally."""

    def __init__(self, input_source: Union[PathLike, str], output_directory: Path):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        input_source : PathLike
          Can be a string or a Path
        output_directory : Path
          The directory where to save the json files.
        """
        self.input_source = input_source
        self.output_directory = output_directory

    @abstractmethod
    def _extract(self) -> Any:
        """
        Extract the data from self.input_source.
        Returns
        -------
        Any
          It's not clear yet whether we'll be processing binary data, dicts,
          API Responses, etc.

        """

    @abstractmethod
    def _transform(self, extracted_source: Any) -> AindCoreModel:
        """
        Transform the data extracted from the extract method.
        Parameters
        ----------
        extracted_source : Any
          Output from _extract method.

        Returns
        -------
        AindCoreModel

        """

    def _load(self, transformed_data: AindCoreModel) -> None:
        """
        Save the AindCoreModel from the transform method.
        Parameters
        ----------
        transformed_data : AindCoreModel

        Returns
        -------
        None

        """
        transformed_data.write_standard_file(output_directory=self.output_directory)

    @staticmethod
    def _run_validation_check(model_instance: AindCoreModel) -> None:
        """
        Check the response contents against either
        aind_data_schema.subject or aind_data_schema.procedures.
        Parameters
        ----------
        model_instance : AindCoreModel
          Contents from the service response.
        """
        try:
            model_instance.model_validate(model_instance.__dict__)
            logging.debug("No validation errors detected.")
        except ValidationError:
            logging.warning(
                "Validation errors were found. This may be due to "
                "mismatched versions or data not found in the "
                "databases.",
                exc_info=True,
            )

    def run_job(self) -> None:
        """
        Run the etl job
        Returns
        -------
        None

        """
        extracted = self._extract()
        transformed = self._transform(extracted_source=extracted)
        self._run_validation_check(transformed)
        self._load(transformed)

    @classmethod
    def from_args(cls, args: list):
        """
        Adds ability to construct settings from a list of arguments.
        Parameters
        ----------
        args : list
        A list of command line arguments to parse.
        """

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-i",
            "--input-source",
            required=True,
            type=str,
            help="URL or directory of source data",
        )
        parser.add_argument(
            "-o",
            "--output-directory",
            required=False,
            default=".",
            type=str,
            help=(
                "Directory to save json file to. Defaults to current working "
                "directory."
            ),
        )
        job_args = parser.parse_args(args)

        return cls(
            input_source=job_args.input_source,
            output_directory=Path(job_args.output_directory),
        )
