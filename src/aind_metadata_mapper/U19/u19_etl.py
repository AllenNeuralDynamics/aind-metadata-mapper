"""Sets up the U19 ingest ETL"""

import logging
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Union

from pydantic import Field
from pydantic_settings import BaseSettings
from aind_metadata_mapper.core import GenericEtl, JobResponse

import pandas as pd
import requests
import json

from aind_data_schema.core.procedures import Surgery, Procedures, Injection, ViralMaterial, TarsVirusIdentifiers, NonViralMaterial, NanojectInjection, Perfusion, Anaesthetic
from datetime import datetime
from aind_data_schema.core.procedures import VolumeUnit, SizeUnit
import glob
from enum import Enum
from decimal import Decimal



class JobSettings(BaseSettings):
    """Data that needs to be input by user."""

    tracking_sheet_path: Path
    tracker_sheet_name: str
    coordinate_sheet_path: Path
    coordinate_sheet_name: str
    output_directory: Optional[Path] = Field(
        default=None,
        description=(
            "Directory where to save the json file to. If None, then json"
            " contents will be returned in the Response message."
        ),
    )
    experimenter_full_name: List[str]
    subjects_to_ingest: List[str] = Field(
        default=None,
        description=(
            "List of subject IDs to ingest. If None,"
            " then all subjects in spreadsheet will be ingested."
        )
    )



DATETIME_FORMAT = "%H:%M:%S %d %b %Y"
LENGTH_FORMAT = "%Hh%Mm%Ss%fms"


class U19Etl(GenericEtl[JobSettings]):
    """Class for MRI ETL process."""

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
        super().__init__(job_settings=job_settings_model)

    def _extract(self, subj_id) -> :
        """Extract the data from the bruker files."""



        return metadata

    def _transform(self, info_row) -> Surgery:
        """Transform the data into the AIND data schema."""

        return Surgery(
            start_date=,
            experimenter_full_name=,
            iacuc_protocol=,
            animal_weight_prior=,
            animal_weight_post=,
            weight_unit=,
            anaesthesia=,
            workstation_id=,
            procedures=,
            notes=,
        )
    
    def download_procedure_file(self, subj_id: str):
        """Download the procedure file for a subject."""
        # Get the procedure file from the U19 server
        request = requests.get(f"http://aind-metadata-service/procedures/{subj_id}")

        if request.status_code == 404:
            logging.error(f"{subj_id} model not found")
            return None

        item = request.json()

        if item['message'] == 'Valid Model.':
            return item['data']
        return None
    
    def extract_procedures(self, procedure_file):
        """Extract the procedures from the procedure file."""

        return [procedure for procedure in procedure_file['subject_procedures']]
    

    def transform_perfusion(self, subj_id: str):
        """Transform the perfusion procedure."""

        return Perfusion(
            protocol_id="dx.doi.org/10.17504/protocols.io.bg5vjy66",
            output_specimen_ids=[subj_id]
        )
    
    def transform_nanoject(self, procedure, row, injection: str):
        """Transform the nanoject procedure."""

        virus = row[injection]['Virus']
        inj_angle = row[injection]['Inj Angle']
        inj_vol = row[injection]['nL']
        coord_ml = row[injection]['M/L']
        coord_ap = row[injection]['A/P']
        coord_depth = row[injection]['D/V']
        hemisphere = row[injection]['Hemisphere']

        virus_titer = None
        if not pd.isna(row[injection]['Titer']):
            virus_titer = Decimal.from_float(float(row[injection]['Titer']))

        virus_lot = row[injection]['Lot']

        mat1 = ViralMaterial(
            name=virus,
            titer=virus_titer,
            tars_identifiers=TarsVirusIdentifiers(
                prep_lot_number=str(virus_lot),
            )
        )

        inj_mats = [mat1]

        return NanojectInjection(
            protocol_id="dx.doi.org/10.17504/protocols.io.bg5vjy66",
            output_specimen_ids=[subj_id]
        )

        

    def run_job(self) -> JobResponse:
        """Run the job and return the response."""

        extracted = self._extract()
        transformed = self._transform(extracted)

        job_response = self._load(
            transformed, self.job_settings.output_directory
        )

        return job_response