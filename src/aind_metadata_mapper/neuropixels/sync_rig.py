"""ETL for the Sync config."""

from pathlib import Path

from aind_data_schema.core.rig import Rig  # type: ignore
from aind_data_schema.models.devices import DAQChannel  # type: ignore
from pydantic import BaseModel

from aind_metadata_mapper.neuropixels import utils
from aind_metadata_mapper.neuropixels.neuropixels_rig import (
    NeuropixelsRigContext,
    NeuropixelsRigEtl,
)


class SyncChannel(BaseModel):
    """Extracted Sync daq channel information."""

    channel_name: str
    channel_index: int
    sample_rate: float


class ExtractContext(NeuropixelsRigContext):
    """Extract context for Sync rig etl."""

    channels: list[SyncChannel]


class SyncRigEtl(NeuropixelsRigEtl):
    """Sync rig ETL class. Extracts information from Sync-related config
    file."""

    def __init__(
        self,
        input_source: Path,
        output_directory: Path,
        config_source: Path,
        sync_daq_name: str = "Sync",
        **kwargs,
    ):
        """Class constructor for Sync rig etl class."""
        super().__init__(input_source, output_directory, **kwargs)
        self.config_source = config_source
        self.sync_daq_name = sync_daq_name

    def _extract(self) -> ExtractContext:
        """Extracts Sync-related daq information from config files."""
        config = utils.load_yaml(self.config_source)
        sample_rate = config["freq"]
        channels = [
            SyncChannel(
                channel_name=name,
                channel_index=line,
                sample_rate=sample_rate,
            )
            for line, name in config["line_labels"].items()
        ]

        return ExtractContext(
            current=super()._extract(),
            channels=channels,
        )

    def _transform(self, extracted_source: ExtractContext) -> Rig:
        """Updates rig model with Sync-related daq information."""
        utils.find_update(
            extracted_source.current.daqs,
            [
                ("name", self.sync_daq_name),
            ],
            channels=[
                DAQChannel(
                    channel_name=sync_channel.channel_name,
                    channel_type="Digital Input",
                    device_name=self.sync_daq_name,
                    event_based_sampling=False,
                    channel_index=sync_channel.channel_index,
                    sample_rate=sync_channel.sample_rate,
                    sample_rate_unit="hertz",
                )
                for sync_channel in extracted_source.channels
            ],
        )

        return super()._transform(extracted_source.current)
