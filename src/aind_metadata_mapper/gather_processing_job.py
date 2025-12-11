"""Entrypoint to create a processing.json file"""

import json
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

    def run_job(self):
        """Write the processing object to a directory."""

        file_name = Processing.default_filename()
        output_path = Path(self.settings.output_directory) / file_name
        json_contents = self.settings.processing.model_dump(mode="json", exclude_none=True)
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
