from pathlib import Path
from pydantic import BaseModel


class MapperJobSettings(BaseModel):
    """Base class for job settings

    This class can be extended to include specific settings
    required for different mapping jobs.
    """

    input_filepath: Path
    output_filepath: Path


class MapperJob:
    """Base class for a mapper"""

    def run_job(self, job_settings: MapperJobSettings) -> None:
        """Run the mapping job.

        To be automatable by the GatherMetadataJob your subclass
        must implement this method. Do not add additional fields to the job_settings.
        
        Your code should read the input_filename and write to output_filename,
        within the metadata_directory.
        """
        raise NotImplementedError("Subclasses should implement this method.")