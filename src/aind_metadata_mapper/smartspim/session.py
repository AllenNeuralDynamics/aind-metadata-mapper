"""SmartSPIM ETL"""

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from aind_data_schema.core import acquisition
from aind_data_schema.core.processing import (
    DataProcess,
    PipelineProcess,
    ProcessName,
)
from aind_data_schema.core.session import Session
from aind_data_schema.models.coordinates import ImageAxis
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from pydantic_settings import BaseSettings

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.gather_metadata import *

from .utils import (
    get_anatomical_direction,
    get_excitation_emission_waves,
    get_session_end,
    make_acq_tiles,
    read_json_as_dict,
)


class JobSettings(BaseSettings):
    """Data to be entered by the user."""

    mouse_id: str
    raw_dataset_path: Path
    output_directory: Path

    # Metadata names
    asi_filename: str = "derivatives/ASI_logging.txt"
    mdata_filename_json: str = "derivatives/metadata.json"

    # Acquisition json params
    experimenter_full_name: List[str]
    instrument_id: str
    axes: dict
    chamber_immersion_medium: str
    chamber_immersion_ri: str
    sample_immersion_medium: str
    sample_immersion_ri: str
    local_storage_directory: str
    notes: Optional[str]


class SmartspimETL(GenericEtl):
    """
    This class contains the methods to write the metadata
    for a SmartSPIM session
    """

    def __init__(self, job_settings: BaseSettings):
        """
        Constructor method

        Parameters
        ----------
        job_settings: BaseSettings
            Job settings for the SmartSPIM ETL

        """
        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)

        else:
            job_settings_model = job_settings

        super().__init__(job_settings=job_settings_model)

    def __create_acquisition(self):
        """
        Creates the acquisition metadata file using
        the metadata generated at the microscope.
        """

        # Path where the channels are stored
        smartspim_channel_root = self.job_settings.raw_dataset_path.joinpath(
            "SmartSPIM"
        )

        # Getting only valid folders
        channels = [
            folder
            for folder in os.listdir(smartspim_channel_root)
            if os.path.isdir(f"{smartspim_channel_root}/{folder}")
        ]

        # Path to metadata files
        asi_file_path_txt = self.job_settings.raw_dataset_path.joinpath(
            self.job_settings.asi_filename
        )

        mdata_path_json = self.job_settings.raw_dataset_path.joinpath(
            self.job_settings.mdata_filename_json
        )

        # ASI file does not exist, needed for acquisition
        if not asi_file_path_txt.exists():
            raise FileNotFoundError(f"File {asi_file_path_txt} does not exist")

        if not mdata_path_json.exists():
            raise FileNotFoundError(f"File {mdata_path_json} does not exist")

        # Getting acquisition metadata from the microscope
        metadata_info = read_json_as_dict(mdata_path_json)
        filter_mapping = get_excitation_emission_waves(channels)

        session_config = metadata_info["session_config"]
        wavelength_config = metadata_info["wavelength_config"]
        tile_config = metadata_info["tile_config"]

        if None in [session_config, wavelength_config, tile_config]:
            raise ValueError("Metadata json is empty")

        metadata_dict = {
            "session_config": session_config,
            "wavelength_config": wavelength_config,
            "tile_config": tile_config,
        }

        if axes is None:
            raise ValueError("Please, check the axes orientation")

        # Getting axis orientation
        axes = [
            ImageAxis(
                name=ax["name"],
                dimension=ax["dimension"],
                direction=get_anatomical_direction(ax["direction"]),
            )
            for ax in self.job_settings.axes
        ]

        regex_date = r"(20[0-9]{2})-([0-9]{2})-([0-9]{2})_([0-9]{2})-([0-9]{2})-([0-9]{2})"
        mouse_date = re.search(regex_date, self.job_settings.raw_dataset_path)

        # Converting to date
        if mouse_date:
            mouse_date = mouse_date.group()
            mouse_date = datetime.strptime(mouse_date, "%Y-%m-%d_%H_%M_%S")

        else:
            raise ValueError("Error while getting mouse date")

        session_end_time = get_session_end(asi_file_path_txt)

        acquisition_model = acquisition.Acquisition(
            experimenter_full_name=self.job_settings.experimenter_full_name,
            specimen_id="",
            subject_id=self.job_settings.mouse_id,
            instrument_id=self.job_settings.instrument_id,
            session_start_time=mouse_date,
            session_end_time=session_end_time,
            tiles=make_acq_tiles(
                metadata_dict=metadata_dict, filter_mapping=filter_mapping
            ),
            axes=axes,
            chamber_immersion=acquisition.Immersion(
                medium=self.job_settings.chamber_immersion_medium,
                refractive_index=self.job_settings.chamber_immersion_ri,
            ),
            sample_immersion=acquisition.Immersion(
                medium=self.job_settings.sample_immersion_medium,
                refractive_index=self.job_settings.sample_immersion_ri,
            ),
            local_storage_directory=self.job_settings.local_storage_directory,
            external_storage_directory="",
            # processing_steps=[],
            notes=self.job_settings.notes,
        )


def main():
    """Main function to test"""
    subject_id = "695464"
    from datetime import datetime

    job_settings = JobSettings(
        metadata_service_domain="http://aind-metadata-service-dev",
        subject_settings=SubjectSettings(subject_id=subject_id),
        procedures_settings=ProceduresSettings(
            subject_id=subject_id,
        ),
        raw_data_description_settings=RawDataDescriptionSettings(
            name="SmartSPIM_695464_2023-10-18_20-30-30",
            project_name="pj_name",
            modality=[Modality.SPIM],
            institution=Organization.AIND,
        ),
        processing_settings=ProcessingSettings(
            pipeline_process=PipelineProcess(
                data_processes=[
                    DataProcess(
                        name=ProcessName.IMAGE_IMPORTING,
                        software_version="test_sfw",
                        start_date_time=datetime.now(),
                        end_date_time=datetime.now(),
                        input_location="",
                        output_location="",
                        code_url="code url",
                        code_version="code version",
                        parameters={},
                        outputs={},
                        notes="",
                    )
                ],
                processor_full_name="Camilo Laiton",
                pipeline_version="1.6",
                pipeline_url="url",
                note="notes",
            )
        ),
        metadata_settings=MetadataSettings(name="name", location="location"),
        directory_to_write_to="./",
    )

    smartspim_etl = GatherMetadataJob(settings=job_settings)
    smartspim_etl.run_job()


if __name__ == "__main__":
    main()
