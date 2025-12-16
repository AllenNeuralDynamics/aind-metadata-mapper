"""Entrypoint to create a processing.json file"""

import json
import logging
import sys
from pathlib import Path

from aind_data_schema.core.processing import Processing
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class JobSettings(
    BaseSettings,
    cli_parse_args=True,
    cli_ignore_unknown_args=True,
    extra="allow",
):
    """
    Settings required to generate a processing.json file."""

    model_config = SettingsConfigDict(env_prefix="PROCESSING_")
    output_directory: str | Path = Field(..., description="Directory to write the file to.")
    processing: Processing = Field(..., description="Processing")


class GatherProcessingJob:
    """Write a processing.json file to a directory"""

    def __init__(self, settings: JobSettings):
        """Class constructor"""
        self.settings = settings

    def load_existing_processing(self) -> Processing | None:
        """Load an existing processing.json file from the output directory, if it exists."""
        file_name = Processing.default_filename()
        existing_path = Path(self.settings.output_directory) / file_name
        if existing_path.exists():
            try:
                with open(existing_path, "r") as f:
                    existing_data = json.load(f)
                return Processing(**existing_data)
            except Exception as e:
                error_msg = ("Failed to load existing processing.json: {e}. ")
            logging.error(error_msg)
            raise e
        return None

    def run_job(self):
        """Write the processing object to a directory."""

        existing_processing = self.load_existing_processing()
        if existing_processing is not None:
            try:
                merged_processing = existing_processing + self.settings.processing
            except Exception as e:
                error_msg = (
                    f"Failed to merge existing processing.json with new processing data: {e}. "
                    "Cannot proceed without risking data loss. Please verify the existing processing.json "
                    "file is valid and compatible with the new processing data."
                )
                logging.error(error_msg)
                raise e
        else:
            merged_processing = self.settings.processing

        file_name = Processing.default_filename()
        output_path = Path(self.settings.output_directory) / file_name
        json_contents = merged_processing.model_dump(mode="json", exclude_none=True)
        with open(output_path, "w") as f:
            json.dump(json_contents, f, indent=3, ensure_ascii=False, sort_keys=True)


if __name__ == "__main__":
    if len(sys.argv[1:]) == 2 and sys.argv[1] == "--job-settings":
        main_job_settings = JobSettings.model_validate_json(sys.argv[2])
    else:
        # noinspection PyArgumentList
        main_job_settings = JobSettings()
    main_job = GatherProcessingJob(settings=main_job_settings)
    main_job.run_job()
