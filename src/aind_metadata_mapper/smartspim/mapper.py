from datetime import datetime
import re
from aind_data_schema.core.acquisition import Acquisition
from aind_metadata_extractor.models import SmartspimModel
from aind_metadata_mapper.models import Mapper


class SmartspimMapper(Mapper):
    """Smartspim Mapper"""
    
    def _transform(self, metadata: SmartspimModel) -> Acquisition:
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