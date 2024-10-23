"""Base ETL class for dynamic_routing rigs."""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

from aind_data_schema.core.rig import Rig  # type: ignore
from pydantic import BaseModel

from aind_metadata_mapper.core import BaseEtl

logger = logging.getLogger(__name__)


class NeuropixelsRigContext(BaseModel):
    """Base context for dynamic_routing rig etl."""

    current: Rig


class NeuropixelsRigEtl(BaseEtl):
    """Neuropixels rig ETL class. Extracts information from rig-related files
    and transforms them into an aind-data-schema rig.Rig instance.
    """

    def __init__(
        self,
        input_source: Path,
        output_directory: Path,
        modification_date: Optional[date] = None,
    ):
        """Class constructor for Neuropixels rig etl class.

        Parameters
        ----------
        input_source : Path
          Can be a string or a Path
        output_directory : Path
          The directory where to save the json files.
        """
        self.input_source: Path = input_source
        self.output_directory = output_directory
        self.modification_date = modification_date

    def _extract(self) -> Rig:
        """Extracts rig-related information from config files."""
        extracted = Rig.model_validate_json(
            self.input_source.read_text(),
        )
        self.initial_model = extracted.model_copy(deep=True)
        return extracted

    def _transform(self, extracted_source: Rig) -> Rig:
        """Transforms extracted rig context into aind-data-schema rig.Rig
        instance.
        """
        if self.initial_model != extracted_source:
            logger.debug("Rig model changed. Updating modification date.")
            self.update_modification_date(extracted_source, self.modification_date)
        else:
            logger.debug("Rig model unchanged. Keeping modification date.")

        return extracted_source

    @classmethod
    def update_modification_date(
        cls,
        extracted_source: Rig,
        modification_date: Optional[date] = None,
    ) -> Rig:
        """Updates modification date and rig id."""
        room_id, rig_name, _ = extracted_source.rig_id.split("_")
        if modification_date is None:
            modification_date = date.today()

        extracted_source.rig_id = (
            f"{room_id}_{rig_name}_{modification_date.strftime('%Y%m%d')}"
        )
        extracted_source.modification_date = modification_date
