from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.components.configs import (
    ImagingConfig,
    Channel,
    DetectorConfig,
    LightEmittingDiodeConfig,
    LaserConfig,
    DeviceConfig,
)
from aind_metadata_extractor.models.smartspim import SmartspimModel
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
        subject_id = metadata.slims_metadata.subject_id

        # fields from metadata_dict
        active_obj = metadata.file_metadata.session_config.get("Obj")

        # fields from slims_data
        specimen_id = metadata.slims_metadata.specimen_id
        instrument_id = metadata.slims_metadata.instrument_id
        protocol_id = metadata.slims_metadata.protocol_id
        experimenter_name = metadata.slims_metadata.experimenter_name
        
        # Build the channels
        
        
        # Build the data stream
        
        imaging_config = ImagingConfig(
            
        )

        acquisition = Acquisition(
            specimen_id=specimen_id,
            subject_id=subject_id,
            acquisition_type="SmartSPIM",
            acquisition_start_time=metadata.file_metadata.session_start_time,
            acquisition_end_time=metadata.file_metadata.session_end_time,
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