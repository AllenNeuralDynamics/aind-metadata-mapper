"""Entrypoint to update an existing DocDB processing record."""

import json
import logging
import os
import sys
from copy import deepcopy
from typing import List, Optional

from aind_data_access_api.document_db import MetadataDbClient
from aind_data_schema.core.processing import Processing
from aind_data_schema_models.modalities import Modality
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


class JobSettings(
    BaseSettings,
    cli_parse_args=True,
    cli_ignore_unknown_args=True,
    extra="allow",
):
    """Settings required for the AppendProcessingJob class."""

    model_config = SettingsConfigDict(env_prefix="PROCESSING_")
    data_asset_id: Optional[str] = Field(
        default=None, description="ID of data asset to update. Either this or s3_location needs to be set."
    )
    s3_location: Optional[str] = Field(
        default=None,
        description="S3 Location of data asset to update. Either this or data_asset_id needs to be set.",
    )
    docdb_host: str = Field(..., description="URL of DocDB host.")
    docdb_version: Optional[str] = Field(default=None, description="Metadata schema major version used.")
    processing: Processing = Field(..., description="Processing")
    modalities: List[Modality.ONE_OF] = Field(
        default=[], description="These modalities will be added to the data description record."
    )

    @model_validator(mode="after")
    def check_s3_location_and_data_asset_id(self) -> "JobSettings":
        """Check that only one of s3_location or data_asset_id is set."""
        has_data_asset_id = self.data_asset_id is not None
        has_s3_location = self.s3_location is not None
        if not (has_data_asset_id or has_s3_location):
            raise ValueError("Either s3_location or data_asset_id is required.")
        elif has_data_asset_id and has_s3_location:
            raise ValueError("Only one of s3_location or data_asset_id should be set.")
        return self


class AppendProcessingJob:
    """Append processing metadata to DocDB record."""

    def __init__(self, settings: JobSettings):
        """Class constructor"""
        self.settings = settings

    def _fetch_docdb_record_info(self, docdb_client: MetadataDbClient) -> dict:
        """Fetch an existing DocDB record using either an s3_location or data_asset_id."""
        if self.settings.s3_location is not None:
            filter_query = {"location": self.settings.s3_location}
        else:
            filter_query = {"_id": self.settings.data_asset_id}
        projection = {"_id": 1, "location": 1, "processing": 1, "data_description": 1}
        records = docdb_client.retrieve_docdb_records(filter_query=filter_query, projection=projection)
        if len(records) > 1:
            raise AssertionError("More than 1 DocDB record found!")
        elif len(records) == 0:
            raise AssertionError("No DocDB record found!")
        return records[0]

    def _update_record(self, docdb_record: dict) -> dict:
        """Update the docdb_record with the new processing and modality info from the job_settings."""
        old_processing = Processing.model_validate_json(json.dumps(docdb_record["processing"]))
        merged_processing = self.settings.processing + old_processing
        new_record = deepcopy(docdb_record)
        old_modalities = docdb_record["data_description"].get("modalities", [])
        old_modality_abbreviations = [m["abbreviation"] for m in old_modalities]
        updated_modalities = deepcopy(old_modalities)
        for modality in self.settings.modalities:
            if modality.abbreviation not in old_modality_abbreviations:
                updated_modalities.append(modality.model_dump(mode="json"))
        updated_modalities = sorted(updated_modalities, key=lambda d: sorted(d.keys()))
        if len(updated_modalities) > 0:
            new_record["data_description"]["modalities"] = updated_modalities
        new_record["processing"] = merged_processing.model_dump(mode="json")
        return new_record

    @staticmethod
    def _save_updated_record(docdb_client: MetadataDbClient, updated_processing_record: dict) -> None:
        """Update the docdb_record with the appended processing info."""
        response = docdb_client.upsert_one_docdb_record(record=updated_processing_record)
        response.raise_for_status()
        return None

    def run_job(self):
        """Append processing information to DocDB record."""
        logger.info("Starting job.")
        docdb_client = MetadataDbClient(version=self.settings.docdb_version, host=self.settings.docdb_host)
        logger.info("Fetching old record.")
        old_record = self._fetch_docdb_record_info(docdb_client=docdb_client)
        logger.info("Merging record.")
        merged_record = self._update_record(docdb_record=old_record)
        logger.info("Updating DocDB record.")
        self._save_updated_record(docdb_client=docdb_client, updated_processing_record=merged_record)
        logger.info("Finished job.")


if __name__ == "__main__":
    if len(sys.argv[1:]) == 2 and sys.argv[1] == "--job-settings":
        main_job_settings = JobSettings.model_validate_json(sys.argv[2])
    else:
        # noinspection PyArgumentList
        main_job_settings = JobSettings()
    main_job = AppendProcessingJob(settings=main_job_settings)
    main_job.run_job()
