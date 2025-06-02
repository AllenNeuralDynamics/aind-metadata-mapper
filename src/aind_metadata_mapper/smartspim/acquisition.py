"""SmartSPIM ETL to map metadata"""

import os
import re
import sys
import requests
import logging
from datetime import datetime
from typing import Dict, Union, List

from aind_data_schema.components.coordinates import ImageAxis
from aind_data_schema.core.acquisition import (
    Acquisition,
    Immersion,
    ProcessingSteps,
)
from aind_data_schema_models.process_names import ProcessName
from aind_data_schema.components.devices import ImmersionMedium

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.smartspim.models import (
    JobSettings,
    SlimsImmersionMedium,
)
from aind_metadata_mapper.smartspim.utils import (
    get_anatomical_direction,
    get_excitation_emission_waves,
    get_session_end,
    make_acq_tiles,
    read_json_as_dict,
    parse_channel_name,
    ensure_list,
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

    def _extract_metadata_from_microscope_files(self) -> Dict:
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

        mdata_path = self.job_settings.input_source.joinpath(
            self.job_settings.mdata_filename_json
        )

        # ASI file does not exist, needed for acquisition
        if not asi_file_path_txt.exists():
            raise FileNotFoundError(f"File {asi_file_path_txt} does not exist")

        if not mdata_path.exists():
            raise FileNotFoundError(f"File {mdata_path} does not exist")

        # Getting acquisition metadata from the microscope
        metadata_info = read_json_as_dict(mdata_path)

        filter_mapping = get_excitation_emission_waves(channels)
        session_config = metadata_info["session_config"]
        wavelength_config = metadata_info["wavelength_config"]
        tile_config = metadata_info["tile_config"]

        if None in [session_config, wavelength_config, tile_config]:
            raise ValueError("Metadata json is empty")

        session_end_time = get_session_end(asi_file_path_txt)
        mdate_match = re.search(
            self.REGEX_DATE, self.job_settings.input_source.stem
        )
        if not (mdate_match):
            raise ValueError("Error while extracting session date.")
        session_start = datetime.strptime(
            mdate_match.group(), "%Y-%m-%d_%H-%M-%S"
        )

        metadata_dict = {
            "session_config": session_config,
            "wavelength_config": wavelength_config,
            "tile_config": tile_config,
            "session_start_time": session_start,
            "session_end_time": session_end_time,
            "filter_mapping": filter_mapping,
        }

        return metadata_dict

    def _extract_metadata_from_slims(
        self, start_date_gte: str = None, end_date_lte: str = None
    ) -> Dict:
        """
        Method to retrieve smartspim imaging info from SLIMS
        using the metadata service endpoint.
        Parameters
        ----------
        start_date_gte: str
            Start date for the search.
        end_date_lte: str
            End date for the search.
        Returns
        -------
        Dict
            Dictionary containing metadata from SLIMS for an acquisition.
        """
        query_params = {"subject_id": self.job_settings.subject_id}
        if start_date_gte:
            query_params["start_date_gte"] = start_date_gte
        if end_date_lte:
            query_params["end_date_lte"] = end_date_lte
        response = requests.get(
            f"{self.job_settings.metadata_service_path}",
            params=query_params,
        )
        response.raise_for_status()
        if (
            response.status_code == 200
            and len(response.json().get("data")) > 1
        ):
            raise ValueError(
                "More than one imaging session found for the same subject_id. "
                "Please refine your search."
            )
        elif (
            response.status_code == 200
            and len(response.json().get("data")) == 1
        ):
            imaging_info = response.json().get("data")[0]
        else:
            imaging_info = {}
        return imaging_info

    def _transform(self, metadata_dict: Dict, slims_data: Dict) -> Acquisition:
        """
        Transforms raw metadata from both microscope files and SLIMS
        into a complete Acquisition model.

        Parameters
        ----------
        metadata_dict : Dict
            Metadata extracted from the microscope files.
        slims_data : Dict
            Metadata fetched from the SLiMS service.

        Returns
        -------
        Acquisition
            Fully composed acquisition model.
        """
        mdate_match = re.search(
            self.REGEX_DATE, self.job_settings.input_source.stem
        )
        mid_match = re.search(
            self.REGEX_MOUSE_ID, self.job_settings.input_source.stem
        )
        if not (mdate_match and mid_match):
            raise ValueError("Error while extracting mouse date and ID")
        session_start = datetime.strptime(
            mdate_match.group(), "%Y-%m-%d_%H-%M-%S"
        )
        subject_id = mid_match.group()

        # fields from metadata_dict
        active_obj = metadata_dict["session_config"].get("Obj")

        # fields from slims_data
        specimen_id = slims_data.get("specimen_id", "")
        instrument_id = slims_data.get("instrument_id")
        protocol_id = slims_data.get("protocol_id")
        experimenter_name = slims_data.get("experimenter_name")

        acquisition = Acquisition(
            specimen_id=specimen_id,
            subject_id=subject_id,
            session_start_time=session_start,
            session_end_time=metadata_dict["session_end_time"],
            tiles=make_acq_tiles(
                metadata_dict=metadata_dict,
                filter_mapping=metadata_dict["filter_mapping"],
            ),
            external_storage_directory="",
            active_objectives=[active_obj] if active_obj else None,
            instrument_id=instrument_id,
            experimenter_full_name=(
                [experimenter_name] if experimenter_name else []
            ),
            protocol_id=[protocol_id] if protocol_id else [],
            chamber_immersion=Immersion(
                medium=self._map_immersion_medium(
                    slims_data.get("chamber_immersion_medium")
                ),
                refractive_index=slims_data.get("chamber_refractive_index"),
            ),
            sample_immersion=Immersion(
                medium=self._map_immersion_medium(
                    slims_data.get("sample_immersion_medium")
                ),
                refractive_index=slims_data.get("sample_refractive_index"),
            ),
            axes=self._map_axes(
                x=slims_data.get("x_direction"),
                y=slims_data.get("y_direction"),
                z=slims_data.get("z_direction"),
            ),
            processing_steps=self._map_processing_steps(slims_data),
        )
        return acquisition

    @staticmethod
    def _map_processing_steps(slims_data: Dict) -> List[ProcessingSteps]:
        """
        Maps the channel info from SLIMS to the ProcessingSteps model.

        Parameters
        ----------
        slims_data: Dict
            Dictionary with the data from the SLIMS database.

        Returns
        -------
        List[ProcessingSteps]
            List of processing steps mapped from SLIMS data.
        """
        imaging = ensure_list(slims_data.get("imaging_channels"))
        stitching = ensure_list(slims_data.get("stitching_channels"))
        ccf_registration = ensure_list(
            slims_data.get("ccf_registration_channels")
        )
        cell_segmentation = ensure_list(
            slims_data.get("cell_segmentation_channels")
        )

        list_to_steps = [
            (
                imaging,
                [
                    ProcessName.IMAGE_DESTRIPING,
                    ProcessName.IMAGE_FLAT_FIELD_CORRECTION,
                    ProcessName.IMAGE_TILE_FUSING,
                ],
            ),
            (stitching, [ProcessName.IMAGE_TILE_ALIGNMENT]),
            (ccf_registration, [ProcessName.IMAGE_ATLAS_ALIGNMENT]),
            (cell_segmentation, [ProcessName.IMAGE_CELL_SEGMENTATION]),
        ]
        step_map: dict[str, set[ProcessName]] = {}

        for channel_list, process_names in list_to_steps:
            for raw_ch in channel_list:
                parsed = parse_channel_name(raw_ch)
                if parsed not in step_map:
                    step_map[parsed] = set()
                step_map[parsed].update(process_names)

        processing_steps: List[ProcessingSteps] = []
        for channel_name, names_set in step_map.items():
            processing_steps.append(
                ProcessingSteps(
                    channel_name=channel_name, process_name=list(names_set)
                )
            )

        return processing_steps

    @staticmethod
    def _map_axes(x: str, y: str, z: str) -> List[ImageAxis]:
        """Maps the axes directions to the ImageAxis enum."""
        x_axis = ImageAxis(
            name="X", dimension=2, direction=get_anatomical_direction(x)
        )
        y_axis = ImageAxis(
            name="Y", dimension=1, direction=get_anatomical_direction(y)
        )
        z_axis = ImageAxis(
            name="Z", dimension=0, direction=get_anatomical_direction(z)
        )
        return [x_axis, y_axis, z_axis]

    @staticmethod
    def _map_immersion_medium(medium: str) -> ImmersionMedium:
        """
        Maps the immersion medium to the ImmersionMedium enum.

        Parameters
        ----------
        medium: str
            The immersion medium to be mapped.

        Returns
        -------
        ImmersionMedium
            The mapped immersion medium.
        """
        if medium == SlimsImmersionMedium.DIH2O.value:
            return ImmersionMedium.WATER
        elif (
            medium == SlimsImmersionMedium.CARGILLE_OIL_152.value
            or medium == SlimsImmersionMedium.CARGILLE_OIL_153.value
        ):
            return ImmersionMedium.OIL
        elif medium == SlimsImmersionMedium.ETHYL_CINNAMATE.value:
            return ImmersionMedium.ECI
        elif medium == SlimsImmersionMedium.EASYINDEX.value:
            return ImmersionMedium.EASYINDEX
        else:
            return ImmersionMedium.OTHER

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
        metadata_dict = self._extract_metadata_from_microscope_files()
        try:
            if self.job_settings.slims_datetime:
                slims_data = self._extract_metadata_from_slims(
                    start_date_gte=self.job_settings.slims_datetime,
                    end_date_lte=self.job_settings.slims_datetime,
                )
            else:
                slims_data = self._extract_metadata_from_slims(
                    start_date_gte=metadata_dict["session_start_time"],
                    end_date_lte=metadata_dict["session_end_time"],
                )
        except Exception as e:
            logging.error("Unexpected error occurred: %s", e)
            raise

        acquisition_model = self._transform(
            metadata_dict=metadata_dict, slims_data=slims_data
        )

        job_response = self._load(
            acquisition_model, self.job_settings.output_directory
        )
        return job_response


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = SmartspimETL(job_settings=main_job_settings)
    etl.run_job()
