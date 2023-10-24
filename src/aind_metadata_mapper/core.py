"""Core abstract class that can be used as a template for etl jobs."""

import argparse
import logging
from abc import ABC, abstractmethod
from os import PathLike
from pathlib import Path
from typing import Any, Union

from aind_data_schema.base import AindCoreModel
from pydantic import validate_model


class BaseEtl(ABC):
    """Base etl class. Defines interface for extracting, transforming, and
    loading input sources into a json file saved locally."""

    def __init__(
        self, input_source: Union[PathLike, str], output_directory: Path
    ):
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
        transformed_data.write_standard_file(
            output_directory=self.output_directory
        )

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
            *_, validation_error = validate_model(
                model_instance.__class__, model_instance.__dict__
            )
            if validation_error:
                logging.warning(
                    f"Validation errors were found. This may be due to "
                    f"mismatched versions or data not found in the "
                    f"databases. Error: {validation_error}"
                )
            else:
                logging.debug("No validation errors detected.")
        except AttributeError as e:
            logging.warning(
                f"An error was detected during the validation check. "
                f"This may be due to mismatched versions of aind-data-schema "
                f"from the source and this local package. {repr(e)}"
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
