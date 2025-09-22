"""Module to gather metadata from different sources."""

import json
import logging
import os

import requests
from aind_data_schema.core.subject import Subject

from aind_metadata_mapper.models import JobSettings


class GatherMetadataJob:
    """Class to handle retrieving metadata"""

    def __init__(self, settings: JobSettings):
        """
        Class constructor
        Parameters
        ----------
        settings : JobSettings
        """
        self.settings = settings

    def _does_file_exist_in_user_defined_dir(self, file_name: str) -> bool:
        """
        Check whether a file exists in a directory.
        Parameters
        ----------
        file_name : str
          Something like subject.json

        Returns
        -------
        True if self.settings.metadata_dir is not None and file is in that dir

        """
        file_path_to_check = os.path.join(
            self.settings.metadata_dir, file_name
        )
        if os.path.isfile(file_path_to_check):
            return True
        else:
            return False

    def _get_file_from_user_defined_directory(self, file_name: str) -> dict:
        """
        Get a file from a user defined directory
        Parameters
        ----------
        file_name : str
          Like subject.json

        Returns
        -------
        File contents as a dictionary

        """
        file_path = os.path.join(self.settings.metadata_dir, file_name)
        with open(file_path, "r") as f:
            contents = json.load(f)
        return contents

    def get_subject(self) -> dict:
        """Get subject metadata"""
        logging.info("Gathering subject metadata.")
        file_name = Subject.default_filename()
        subject_id = self.settings.subject_id
        if not self._does_file_exist_in_user_defined_dir(file_name=file_name):
            logging.debug(
                f"No subject file found in directory. Downloading "
                f"{self.settings.subject_id} from "
                f"{self.settings.metadata_service_url}"
            )
            response = requests.get(
                f"{self.settings.metadata_service_url}"
                f"/api/v2/subject/{subject_id}"
            )
            if response.status_code not in [200, 400]:
                response.raise_for_status()
            contents = response.json()
        else:
            logging.debug(f"Using existing {file_name}.")
            contents = self._get_file_from_user_defined_directory(
                file_name=file_name
            )
        return contents

    def run_job(self) -> None:
        """Run job"""
        logging.info("Starting run_job")
        core_metadata = dict()
        core_metadata[Subject.default_filename()] = self.get_subject()

        for k, v in core_metadata.items():
            logging.debug(f"Writing {k} file")
            output_file = os.path.join(self.settings.metadata_dir, k)
            with open(output_file, "w") as f:
                json.dump(v, f, indent=3, ensure_ascii=False, sort_keys=True)
        logging.info("Finished job.")


if __name__ == "__main__":
    main_job_settings = JobSettings()
    job = GatherMetadataJob(settings=main_job_settings)
    job.run_job()
