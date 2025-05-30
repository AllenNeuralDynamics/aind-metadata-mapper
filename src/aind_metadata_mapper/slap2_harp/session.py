
import logging
from pathlib import Path
from typing import Union

from aind_data_schema.core.session import Session
from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.open_ephys.models import JobSettings

logger = logging.getLogger(__name__)

class Slap2HarpSessionEtl(GenericEtl):
    """
    A generic ETL for SLAP2 Harp sessions.
    """

    session_path: Path
    output_dir: Path

    def __init__(self, job_settings: Union[JobSettings, str, dict]) -> None:
        """
        Initialize ETL with job settings.
        """
        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        elif isinstance(job_settings, dict):
            job_settings_model = JobSettings(**job_settings)
        else:
            job_settings_model = job_settings
        GenericEtl.__init__(self, job_settings=job_settings_model)

        self.session_path = job_settings.input_source
        self.output_dir = job_settings.output_directory
        logger.debug(f"Initialized SLAP2 Harp ETL for {self.session_path}")

    def run_job(self):
        """Transforms all metadata for the session into relevant files"""
        self._extract()
        self._transform()
        return self._load(self.session_json, self.output_dir)

    def _extract(self):
        """
        Extract raw data and metadata from session files.
        """
        # TODO: Implement extraction logic for SLAP2 Harp
        logger.debug("Extracting data for SLAP2 Harp session.")

    def _transform(self) -> Session:
        """
        Transform extracted data into Session schema.
        """
        # TODO: Implement transformation logic for SLAP2 Harp
        self.session_json = Session(
            experimenter_full_name=[],
            session_start_time=None,
            session_end_time=None,
            session_type=self.job_settings.session_type,
            iacuc_protocol=self.job_settings.iacuc_protocol,
            rig_id=None,
            subject_id=None,
            data_streams=[],
            stimulus_epochs=[],
            mouse_platform_name=None,
            active_mouse_platform=None,
            reward_consumed_unit=None,
            notes="",
        )
        logger.debug("Transformed data into Session schema.")
        return self.session_json

    # Add additional methods as needed for SLAP2 Harp specifics

def main() -> None:
    """
    Run Main
    """
    # Replace 'vars' with actual job settings or argument parsing
    sessionETL = Slap2HarpSessionEtl(**vars)
    sessionETL.run_job()

if __name__ == "__main__":
    main()
