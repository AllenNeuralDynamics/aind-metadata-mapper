"""SmartSPIM ETL to map metadata"""

import os
import re
import sys
from datetime import datetime
from typing import Dict, Union

from aind_data_schema.components.coordinates import ImageAxis
from aind_data_schema.core import acquisition

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.smartspim.models import JobSettings
from aind_metadata_mapper.smartspim.utils import (
    get_anatomical_direction,
    get_excitation_emission_waves,
    get_session_end,
    make_acq_tiles,
    read_json_as_dict,
)


class SmartspimETL(GenericEtl[JobSettings]):
    """
    This class contains the methods to write the metadata
    for a SmartSPIM session
    """

    # TODO: Deprecate this constructor. Use GenericEtl constructor instead
    def __init__(self, job_settings: Union[JobSettings, str]):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        job_settings: Union[JobSettings, str]
          Variables for a particular session
        """

        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        else:
            job_settings_model = job_settings
        if (
            job_settings_model.raw_dataset_path is not None
            and job_settings_model.input_source is None
        ):
            job_settings_model.input_source = (
                job_settings_model.raw_dataset_path
            )
        super().__init__(job_settings=job_settings_model)

    REGEX_DATE = (
        r"(20[0-9]{2})-([0-9]{2})-([0-9]{2})_([0-9]{2})-"
        r"([0-9]{2})-([0-9]{2})"
    )
    REGEX_MOUSE_ID = r"([0-9]{6})"

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
        smartspim_channel_root = self.job_settings.input_source.joinpath(
            "SmartSPIM"
        )

        # Getting only valid folders
        channels = [
            folder
            for folder in os.listdir(smartspim_channel_root)
            if os.path.isdir(f"{smartspim_channel_root}/{folder}")
        ]

        # Path to metadata files
        asi_file_path_txt = self.job_settings.input_source.joinpath(
            self.job_settings.asi_filename
        )

        mdata_path_json = self.job_settings.input_source.joinpath(
            self.job_settings.mdata_filename_json
        )

        processing_manifest_path = self.job_settings.input_source.joinpath(
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
        to create the acquisition.json defined on the
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
            self.REGEX_DATE, self.job_settings.input_source.stem
        )
        mouse_id = re.search(
            self.REGEX_MOUSE_ID, self.job_settings.input_source.stem
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

        if chamber_immersion is None or sample_immersion is None:
            raise ValueError("Please, provide the immersion mediums.")

        chm_medium = chamber_immersion.get("medium")
        spl_medium = sample_immersion.get("medium")

        # Parsing the mediums the operator gives
        notes = (
            f"Chamber immersion: {chm_medium} - Sample immersion: {spl_medium}"
        )
        notes += f" - Operator notes: {processing_manifest.get('notes')}"

        if "cargille" in chm_medium.lower():
            chm_medium = "oil"

        else:
            chm_medium = "other"

        if "cargille" in spl_medium.lower():
            spl_medium = "oil"

        else:
            spl_medium = "other"

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
                medium=chm_medium,
                refractive_index=chamber_immersion.get("refractive_index"),
            ),
            sample_immersion=acquisition.Immersion(
                medium=spl_medium,
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
        return job_response


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = SmartspimETL(job_settings=main_job_settings)
    etl.run_job()
