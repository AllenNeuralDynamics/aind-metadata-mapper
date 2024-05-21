"""SmartSPIM ETL to map metadata"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict

from aind_data_schema.components.coordinates import ImageAxis
from aind_data_schema.core import acquisition
from pydantic_settings import BaseSettings
from utils import (
    get_anatomical_direction,
    get_excitation_emission_waves,
    get_session_end,
    make_acq_tiles,
    read_json_as_dict,
)

from aind_metadata_mapper.core import GenericEtl, JobResponse

# from aind_metadata_mapper.gather_metadata import GatherMetadataJob
# from aind_metadata_mapper.gather_metadata import (
#     JobSettings as MetadataJobSettings,
# )
# from aind_metadata_mapper.gather_metadata import (
#     MetadataSettings,
#     ProceduresSettings,
#     ProcessingSettings,
#     RawDataDescriptionSettings,
#     SubjectSettings,
# )


class JobSettings(BaseSettings):
    """Data to be entered by the user."""

    subject_id: str
    raw_dataset_path: Path
    output_directory: Path

    # Metadata names
    asi_filename: str = "derivatives/ASI_logging.txt"
    mdata_filename_json: str = "derivatives/metadata.json"

    # Metadata provided by microscope operators
    processing_manifest_path: str = "derivatives/processing_manifest.json"


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

        self.regex_date = r"(20[0-9]{2})-([0-9]{2})-([0-9]{2})_([0-9]{2})-([0-9]{2})-([0-9]{2})"
        self.regex_mouse_id = r"([0-9]{6})"

        super().__init__(job_settings=job_settings_model)

    def _extract(self) -> Dict:
        """
        Extracts metadata from the microscope metadata files.

        Returns
        -------
        Dict
            Dictionary containing metadata from
            the microscope for the current acquisition. This
            is needed to build the acquisition.json.
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

        processing_manifest_path = self.job_settings.raw_dataset_path.joinpath(
            self.job_settings.processing_manifest_path
        )

        # ASI file does not exist, needed for acquisition
        if not asi_file_path_txt.exists():
            raise FileNotFoundError(f"File {asi_file_path_txt} does not exist")

        if not mdata_path_json.exists():
            raise FileNotFoundError(f"File {mdata_path_json} does not exist")

        if not processing_manifest_path.exists():
            raise FileNotFoundError(
                f"File {processing_manifest_path} does not exist"
            )

        # Getting acquisition metadata from the microscope
        metadata_info = read_json_as_dict(mdata_path_json)
        processing_manifest = read_json_as_dict(processing_manifest_path)

        filter_mapping = get_excitation_emission_waves(channels)

        session_config = metadata_info["session_config"]
        wavelength_config = metadata_info["wavelength_config"]
        tile_config = metadata_info["tile_config"]

        if None in [session_config, wavelength_config, tile_config]:
            raise ValueError("Metadata json is empty")

        session_end_time = get_session_end(asi_file_path_txt)

        metadata_dict = {
            "session_config": session_config,
            "wavelength_config": wavelength_config,
            "tile_config": tile_config,
            "session_end_time": session_end_time,
            "filter_mapping": filter_mapping,
            "processing_manifest": processing_manifest,
        }

        return metadata_dict

    def _transform(self, metadata_dict: Dict) -> acquisition.Acquisition:
        """
        Transforms the raw metadata from the acquisition
        to create the acquition.json defined on the
        aind-data-schema package.

        Parameters
        ----------
        metadata_dict: Dict
            Dictionary with the metadata extracted from
            the microscope files.

        Returns
        -------
        Acquisition
            Built acquisition model.
        """

        mouse_date = re.search(
            self.regex_date, self.job_settings.raw_dataset_path.stem
        )
        mouse_id = re.search(
            self.regex_mouse_id, self.job_settings.raw_dataset_path.stem
        )

        # Converting to date and mouse ID
        if mouse_date and mouse_id:
            mouse_date = mouse_date.group()
            mouse_date = datetime.strptime(mouse_date, "%Y-%m-%d_%H-%M-%S")

            mouse_id = mouse_id.group()

        else:
            raise ValueError("Error while getting mouse date and ID")

        processing_manifest = metadata_dict["processing_manifest"][
            "prelim_acquisition"
        ]
        axes = processing_manifest.get("axes")

        if axes is None:
            raise ValueError("Please, check the axes orientation")

        # Getting axis orientation
        axes = [
            ImageAxis(
                name=ax["name"],
                dimension=ax["dimension"],
                direction=get_anatomical_direction(ax["direction"]),
            )
            for ax in axes
        ]

        chamber_immersion = processing_manifest.get("chamber_immersion")
        sample_immersion = processing_manifest.get("sample_immersion")

        chamber_immersion_medium = chamber_immersion.get("medium")
        sample_immersion_medium = sample_immersion.get("medium")

        if chamber_immersion is None or sample_immersion is None:
            raise ValueError("Please, provide the immersion mediums.")

        # Parsing the mediums the operator gives
        notes = f"Chamber immersion: {chamber_immersion_medium} - Sample immersion: {sample_immersion_medium}"
        notes += f" - Operator notes: {processing_manifest.get('notes')}"

        if "cargille" in chamber_immersion_medium.lower():
            chamber_immersion_medium = "oil"

        else:
            chamber_immersion_medium = "other"

        if "cargille" in sample_immersion_medium.lower():
            sample_immersion_medium = "oil"

        else:
            sample_immersion_medium = "other"

        acquisition_model = acquisition.Acquisition(
            experimenter_full_name=processing_manifest.get(
                "experimenter_full_name"
            ),
            specimen_id="",
            subject_id=mouse_id,
            instrument_id=processing_manifest.get("instrument_id"),
            session_start_time=mouse_date,
            session_end_time=metadata_dict["session_end_time"],
            tiles=make_acq_tiles(
                metadata_dict=metadata_dict,
                filter_mapping=metadata_dict["filter_mapping"],
            ),
            axes=axes,
            chamber_immersion=acquisition.Immersion(
                medium=chamber_immersion_medium,
                refractive_index=chamber_immersion.get("refractive_index"),
            ),
            sample_immersion=acquisition.Immersion(
                medium=sample_immersion_medium,
                refractive_index=sample_immersion.get("refractive_index"),
            ),
            local_storage_directory=processing_manifest.get(
                "local_storage_directory"
            ),
            external_storage_directory="",
            # processing_steps=[],
            notes=notes,
        )

        return acquisition_model

    def run_job(self) -> JobResponse:
        """
        Runs the SmartSPIM ETL job.

        Returns
        -------
        JobResponse
            The JobResponse object with information about the model. The
            status_codes are:
            200 - No validation errors on the model and written without errors
            406 - There were validation errors on the model
            500 - There were errors writing the model to output_directory

        """
        metadata_dict = self._extract()
        acquisition_model = self._transform(metadata_dict=metadata_dict)
        job_response = self._load(
            acquisition_model, self.job_settings.output_directory
        )
        print(job_response)
        return job_response


def main():
    """Main function to test"""

    subject_id = "695464"
    dataset_path = "/allen/aind/scratch/svc_aind_upload/test_data_sets/smartspim/SmartSPIM_695464_2023-10-18_20-30-30"
    output_directory = "/allen/aind/scratch/svc_aind_upload/test_data_sets/smartspim/test_outputs"

    job_setts = JobSettings(
        subject_id=subject_id,
        raw_dataset_path=dataset_path,
        output_directory=output_directory,
        asi_filename="derivatives/ASI_logging.txt",
        mdata_filename_json="derivatives/metadata.json",
        processing_manifest_path="derivatives/processing_manifest.json",
    )

    smartspim_job = SmartspimETL(job_settings=job_setts)
    smartspim_job.run_job()

    # mdata_job_settings = MetadataJobSettings(
    #     metadata_service_domain="http://aind-metadata-service-dev",
    #     subject_settings=SubjectSettings(subject_id=subject_id),
    #     procedures_settings=ProceduresSettings(
    #         subject_id=subject_id,
    #     ),
    #     raw_data_description_settings=RawDataDescriptionSettings(
    #         name="SmartSPIM_695464_2023-10-18_20-30-30",
    #         project_name="pj_name",
    #         modality=[Modality.SPIM],
    #         institution=Organization.AIND,
    #     ),
    #     processing_settings=ProcessingSettings(
    #         pipeline_process=PipelineProcess(
    #             data_processes=[
    #                 DataProcess(
    #                     name=ProcessName.IMAGE_IMPORTING,
    #                     software_version="test_sfw",
    #                     start_date_time=datetime.now(),
    #                     end_date_time=datetime.now(),
    #                     input_location="",
    #                     output_location="",
    #                     code_url="code url",
    #                     code_version="code version",
    #                     parameters={},
    #                     outputs={},
    #                     notes="",
    #                 )
    #             ],
    #             processor_full_name="Camilo Laiton",
    #             pipeline_version="1.6",
    #             pipeline_url="url",
    #             note="notes",
    #         )
    #     ),
    #     metadata_settings=MetadataSettings(name="name", location="location"),
    #     directory_to_write_to=output_directory,
    # )

    # metadata_gathering = GatherMetadataJob(settings=mdata_job_settings)
    # metadata_gathering.run_job()


if __name__ == "__main__":
    main()
