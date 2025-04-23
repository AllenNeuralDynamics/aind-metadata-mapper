"""SmartSPIM ETL to map metadata"""

import os
import re
import sys
import requests
from datetime import datetime
from typing import Dict, Union, List, Any
from urllib.parse import quote

from aind_data_schema.components.coordinates import ImageAxis
from aind_data_schema.core.acquisition import Acquisition, Immersion, ProcessingSteps
from aind_data_schema_models.process_names import ProcessName
from aind_data_schema.components.devices import ImmersionMedium

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
from enum import Enum

#TODO: consider moving Slims Handlers to utils 
class SlimsImmersionMedium(Enum):
    """Enum for the immersion medium used in SLIMS."""

    DIH2O = "diH2O"
    CARGILLE_OIL_152 = "Cargille Oil 1.5200"
    CARGILLE_OIL_153 = "Cargille Oil 1.5300"
    ETHYL_CINNAMATE = "ethyl cinnamate"
    OPTIPLEX_DMSO = "Optiplex and DMSO"
    EASYINDEX = "EasyIndex"
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

        session_end_time = get_session_end(asi_file_path_txt)

        metadata_dict = {
            "session_config": session_config,
            "wavelength_config": wavelength_config,
            "tile_config": tile_config,
            "session_end_time": session_end_time,
            "filter_mapping": filter_mapping,
        }

        return metadata_dict

    def _transform(self, metadata_dict: Dict) -> Acquisition:
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

        active_objective = metadata_dict["session_config"].get("Obj", None)

        # create incomplete acquisition model
        acquisition_model = Acquisition.model_construct(
            specimen_id="",
            subject_id=mouse_id,
            session_start_time=mouse_date,
            session_end_time=metadata_dict["session_end_time"],
            tiles=make_acq_tiles(
                metadata_dict=metadata_dict,
                filter_mapping=metadata_dict["filter_mapping"],
            ),
            external_storage_directory="",
            active_objectives=[active_objective] if active_objective else None,
            processing_steps=[],
        )

        return acquisition_model
    
    @staticmethod
    def get_smartspim_imaging_info(
                domain: str, url_path: str, subject_id: str,
                start_date_gte: str = None, end_date_lte: str = None
            ):
            """Utility method to retrieve smartspim imaging info from metadata service"""
            query_params = {"subject_id": subject_id}
            if start_date_gte:
                query_params["start_date_gte"] = start_date_gte
            if end_date_lte:
                query_params["end_date_lte"] = end_date_lte
            response = requests.get(f"{domain}/{url_path}", params=query_params)
            response.raise_for_status()
            if response.status_code == 200:
                imaging_info = response.json().get("data")
            else:
                imaging_info = []
            return imaging_info

    def _integrate_data_from_slims(
            self,
            slims_data: Dict,
            acquisition_model: Acquisition,
    ) -> Acquisition:
        """
        Integrates the data from the SLIMS database into the acquisition model.

        Parameters
        ----------
        slims_data: Dict
            Dictionary with the data from the SLIMS database.

        acquisition_model: Acquisition
            Acquisition model to be integrated with the SLiMS data.

        Returns
        -------
        Acquisition
            The integrated acquisition model.
        """
        protocol_id = slims_data.get("protocol_id", None)
        experimenter_name = slims_data.get("experimenter_name", None)
        acquisition_model.specimen_id = slims_data.get("specimen_id", None)
        acquisition_model.instrument_id = slims_data.get(
            "instrument_id", None
        )
        acquisition_model.experimenter_full_name = [experimenter_name] if experimenter_name else []
        acquisition_model.protocol_id = [protocol_id] if protocol_id else []
        chamber_immersion_medium = slims_data.get("chamber_immersion_medium", None)
        chamber_refractive_index = slims_data.get(
            "chamber_refractive_index", None
        )
        acquisition_model.chamber_immersion = Immersion(
            medium=self._map_immersion_medium(chamber_immersion_medium), 
            refractive_index=chamber_refractive_index,
        )
        sample_immersion_medium = slims_data.get("sample_immersion_medium", None)
        sample_refractive_index = slims_data.get(
            "sample_refractive_index", None
        )
        acquisition_model.sample_immersion = Immersion(
            medium=self._map_immersion_medium(sample_immersion_medium),
            refractive_index=sample_refractive_index,
        )
        acquisition_model.axes = self._map_axes(
            x = slims_data.get("x_direction", None),
            y = slims_data.get("y_direction", None),
            z = slims_data.get("z_direction", None),
        )
        acquisition_model.processing_steps = self._map_processing_steps(
            slims_data=slims_data
        )
        return Acquisition.model_validate(acquisition_model)

    # Definitely move this to utils 
    @staticmethod
    def _ensure_list(raw: Any) -> List[Any]:
        """
        Turn a value that might be a list, a single string, or None
        into a proper list of strings (or an empty list).
        """
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str) and raw.strip():
            return [raw]
        return []
    
    def _map_processing_steps(self, slims_data: Dict) -> List[ProcessingSteps]:
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
        imaging = self._ensure_list(slims_data.get("imaging_channels"))
        stitching = self._ensure_list(slims_data.get("stitching_channels"))
        ccf_registration = self._ensure_list(slims_data.get("ccf_registration_channels"))
        cell_segmentation = self._ensure_list(slims_data.get("cell_segmentation_channels"))

        list_to_steps = [
            (imaging, [
                ProcessName.IMAGE_DESTRIPING,
                ProcessName.IMAGE_FLAT_FIELD_CORRECTION,
                ProcessName.IMAGE_TILE_FUSING,
            ]),
            (stitching, [ ProcessName.IMAGE_TILE_ALIGNMENT ]),
            (ccf_registration, [ ProcessName.IMAGE_ATLAS_ALIGNMENT ]),
            (cell_segmentation, [ ProcessName.IMAGE_CELL_SEGMENTATION ]),
        ]
        step_map: dict[str, set[ProcessName]] = {}

        for channel_list, process_names in list_to_steps:
            for raw_ch in channel_list:
                parsed = self._parse_channel_name(raw_ch)
                if parsed not in step_map:
                    step_map[parsed] = set()
                step_map[parsed].update(process_names)

        processing_steps: List[ProcessingSteps] = []
        for channel_name, names_set in step_map.items():
            processing_steps.append(
                ProcessingSteps(
                    channel_name=channel_name,
                    process_name=list(names_set)
                )
            )

        return processing_steps
    
    @staticmethod
    def _parse_channel_name(channel_str: str) -> str:
        """
        Parses the channel string from SLIMS to a standard format.

        Parameters
        ----------
        channel_str: str
            The channel name to be parsed (ex: "Laser = 445; Emission Filter = 469/35").

        Returns
        -------
        str
            The parsed channel name (ex: "Ex_445_Em_469").
        """
        s = channel_str.replace("Laser", "Ex") \
                   .replace("Emission Filter", "Em")
        parts = [p.strip() for p in re.split(r"[;,]", s) if p.strip()]
        segments = []
        for part in parts:
            key, val = [t.strip() for t in part.split("=", 1)]
            # discard any bandwidth info after slash
            core = val.split("/", 1)[0]
            segments.append(f"{key}_{core}")

        return "_".join(segments)


    @staticmethod
    def _map_axes(x: str, y: str, z: str) -> List[ImageAxis]:
        """Maps the axes directions to the ImageAxis enum."""
        x_axis = ImageAxis(name="X", dimension=2, direction=get_anatomical_direction(x))
        y_axis = ImageAxis(name="Y", dimension=1, direction=get_anatomical_direction(y))
        z_axis = ImageAxis(name="Z", dimension=0, direction=get_anatomical_direction(z))
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
        elif medium == SlimsImmersionMedium.CARGILLE_OIL_152.value or medium == SlimsImmersionMedium.CARGILLE_OIL_153.value:
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
        metadata_dict = self._extract()
        acquisition_model = self._transform(metadata_dict=metadata_dict)
        slims_data=self.get_smartspim_imaging_info(
                domain=self.job_settings.metadata_service_domain,
                url_path=self.job_settings.metadata_service_path,
                subject_id=self.job_settings.subject_id,
                start_date_gte=acquisition_model.session_start_time.isoformat(),
                end_date_lte=acquisition_model.session_end_time.isoformat(),
        )
        if slims_data and len(slims_data) == 1:
            slims_data = slims_data[0]
            acquisition_model = self._integrate_data_from_slims(
                slims_data=slims_data,
                acquisition_model=acquisition_model,
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
