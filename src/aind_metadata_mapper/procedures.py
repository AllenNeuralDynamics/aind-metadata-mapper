"""Module to retrieve procedures metadata and save it to a local json file."""

import argparse
import logging
import sys
from os import PathLike
from pathlib import Path

from aind_data_schema.procedures import Procedures
from aind_metadata_service.client import AindMetadataServiceClient, StatusCodes
from requests import Response

from aind_metadata_mapper.core import BaseEtl


class ProceduresEtl(BaseEtl):
    """Class to retrieve procedures metadata and save it to local json file."""

    def __init__(
        self, input_source: PathLike, output_directory: Path, subject_id: str
    ):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        input_source : PathLike
          Can be a string or a Path
        output_directory : Path
          The directory where to save the json files.
        subject_id : str
          Subject id to pull metadata for
        """
        super().__init__(input_source, output_directory)
        self.subject_id = subject_id

    def _extract(self) -> Response:
        """Query metadata service for a response"""
        domain = str(self.input_source)
        service_client = AindMetadataServiceClient(domain=domain)
        response = service_client.get_procedures(self.subject_id)
        return response

    def _transform(self, extracted_source: Response) -> Procedures:
        """
        Transforms a response from aind-metadata-service into Procedures model
        Parameters
        ----------
        extracted_source : Response

        Returns
        -------
        Procedures
        """
        status_code = StatusCodes(extracted_source.status_code)
        match status_code:
            case StatusCodes.VALID_DATA | StatusCodes.MULTI_STATUS:
                contents = extracted_source.json()["data"]
            case StatusCodes.MULTIPLE_RESPONSES:
                logging.warning(
                    f"Procedures: {extracted_source.json()['message']}"
                )
                contents = extracted_source.json()["data"][0]
            case StatusCodes.INVALID_DATA:
                logging.warning(
                    f"Procedures: {extracted_source.json()['message']}"
                )
                contents = extracted_source.json()["data"]
            case StatusCodes.NO_DATA_FOUND:
                raise Exception(
                    f"No data found for subject id: {self.subject_id}."
                )
            case StatusCodes.INTERNAL_SERVER_ERROR:
                raise Exception("The server reported back an internal error.")
            case _:
                raise Exception("An error occurred connecting to the server.")

        procedures = Procedures.construct(**contents)
        return procedures

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
            required=False,
            type=str,
            default="http://aind-metadata-service",
            help="Domain of source data",
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
        parser.add_argument(
            "-s",
            "--subject-id",
            required=True,
            type=str,
            help="ID of the subject to retrieve metadata for",
        )
        job_args = parser.parse_args(args)

        return cls(
            input_source=job_args.input_source,
            output_directory=Path(job_args.output_directory),
            subject_id=job_args.subject_id,
        )


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    etl = ProceduresEtl.from_args(sys_args)
    etl.run_job()
