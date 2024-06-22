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
    tracking_sheet_name: str
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

    def run_job(self) -> JobResponse:
        """Run the job and return the response."""

        extracted = self._extract()
        transformed = self._transform(extracted)

        job_response = self._load(
            transformed, self.job_settings.output_directory
        )

        return job_response

    def _extract(self) -> dict:
        """Extract the data from the bruker files."""

        self.tracker_sheet = pd.read_excel(self.job_settings.tracking_sheet_path, sheet_name=self.job_settings.tracking_sheet_name, header=[0,1], converters={("SubjInfo", "Mouse ID"): str})
        self.coords_sheet = pd.read_excel(self.job_settings.coordinate_sheet_path, sheet_name=self.job_settings.coordinate_sheet_name)

        procedure_files = {}

        for row_idx, row in self.tracker_sheet.iterrows():
            if not pd.isna(row['Neuroglancer']['Link']):
                subj_id = str(row['SubjInfo']['Mouse ID']).strip().lower()
                logging.info(f"Extracting data for {subj_id}.")
                print(subj_id)
                procedure_files[subj_id] = self.download_procedure_file(subj_id)
            else:
                logging.warning(f"Neuroglancer link missing for {subj_id}.")
                continue

        return procedure_files

    def _transform(self, info_row) -> Surgery:
        """Transform the data into the AIND data schema."""

        pass
    
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
    
    def extract_procedures(self, subj_id, procedure_file):
        """Extract the procedures from the procedure file."""

        self.procedures[subj_id] = [procedure for procedure in procedure_file['subject_procedures']]
    
    def extract_coordinates(self, coordinate_file):
        """Extract the coordinates from the coordinate file."""
        self.coord_values = {}

        for row_idx, row in coordinate_file.iterrows():
            self.coord_values[row['Coord.']] = row['Pax. Structure']
    

    def transform_perfusion(self, subj_id: str):
        """Transform the perfusion procedure."""

        return Perfusion(
            protocol_id="dx.doi.org/10.17504/protocols.io.bg5vjy66",
            output_specimen_ids=[subj_id]
        )
    
    def transform_nanoject(self, subj_id, existing_procedure, row, injection: str):
        """Transform the nanoject procedure."""

        virus = row[injection]['Virus']
        if pd.isnull(virus):
            return None
        
        inj_angle = Decimal(str(row[injection]['Inj Angle']))
        inj_vol = Decimal(str(row[injection]['nL']))
        coord_ml = Decimal(str(row[injection]['M/L']))
        coord_ap = Decimal(str(row[injection]['A/P']))
        coord_depth = Decimal(str(row[injection]['D/V']))
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

        for key, value in [
            ('injection_volume', inj_vol),
            ('injection_angle', inj_angle),
            ('injection_coordinate_ml', coord_ml),
            ('injection_coordinate_ap', coord_ap),
            ('injection_coordinate_depth', coord_depth),
            ('injection_hemisphere', hemisphere),
        ]:
            if not self.matched_value(value, key, existing_procedure):
                existing_procedure[key] = value

        for item in [inj_vol, coord_depth,]:
            if type(item) is not list:
                item = [item]

        return NanojectInjection(
            injection_materials=inj_mats,
            recovery_time=existing_procedure['recovery_time'],
            recovery_time_unit=existing_procedure['recovery_time_unit'],
            injection_duration=existing_procedure['injection_duration'],
            injection_duration_unit=existing_procedure['injection_duration_unit'],
            instrument_id=existing_procedure['instrument_id'],
            protocol_id="dx.doi.org/10.17504/protocols.io.bgpujvnw",
            injection_volume=inj_vol,
            injection_volume_unit=VolumeUnit.NL,
            injection_coordinate_ml=coord_ml,
            injection_coordinate_ap=coord_ap,
            injection_coordinate_depth=coord_depth,
            injection_coordinate_unit=SizeUnit.MM,
            injection_coordinate_reference=existing_procedure['injection_coordinate_reference'],
            bregma_to_lambda_distance=existing_procedure['bregma_to_lambda_distance'],
            bregma_to_lambda_unit=existing_procedure['bregma_to_lambda_unit'],
            inj_angle=inj_angle,
            injection_angle_unit="degrees",
            targeted_structure='Isocortex',
            injection_hemisphere=hemisphere
        )
    
    def transform_row(self, subj_id: str, row):
        subj_id = str(row['SubjInfo']['Mouse ID']).strip().lower()
        logging.info(f"Transforming row for {subj_id}.")

        generated_procedures = {}

        procedure_file = self.download_procedure_file(subj_id)
        if procedure_file:
            procedures = self.extract_procedures(procedure_file)

        headers = ['Perfusion', 'Inj 1', 'Inj 2', 'Inj 3']

        if procedures[subj_id][0]["procedure_type"] != 'Perfusion':
            headers = ['Inj 1', 'Inj 2', 'Inj 3']

        for proc_idx, inj in enumerate(headers):
            if proc_idx < len(procedures):
                cur_procedure = procedures[proc_idx]
            else:
                continue

            logging.info(f"loaded {subj_id} - procedure: {cur_procedure['procedure_type']} - expected: {inj} - data: {cur_procedure}")

            date = row[inj]['Date']

            if not pd.isnull(date):
                date = row[inj]['Date'].date()

            if procedure_file['start_date'] is None:
                procedure_file['start_date'] = date
            else:
                procedure_file['start_date'] = datetime.strptime(procedure_file["start_date"], "%Y-%m-%d").date()

            if not self.matched_value(date, 'start_date', row):
                logging.info(f"DATE MISSMATCH: {date} - {row['start_date']}")

            if inj != 'Perfusion':
                # Not using this for some reason. Mathew thinks the targetting is too specific, and has blanketted all of them to Isocortex
                target_name = row['Coordinate Targets'][inj]
                target = self.coord_values[target_name]

            if procedure_file['procedure_type'] == 'Perfusion':
                cur_generated = self.transform_perfusion(subj_id)
            elif procedure_file['procedure_type'] == 'Nanoject Injection':
                cur_generated = self.transform_nanoject(subj_id, cur_procedure, row, inj)
            else:
                logging.error(f"Unknown procedure type: {procedure_file['procedure_type']} for {subj_id}.")
                continue


            if date in generated_procedures.keys():
                generated_procedures[date].append(cur_generated)
            else:
                generated_procedures[date] = [cur_generated]

        generated_surgeries = []
        for date, procedures in generated_procedures.items():
            generated_surgeries.append(
                Surgery(
                    start_date=date,
                    experimenter_full_name=self.job_settings.experimenter_full_name,
                    iacuc_protocol=procedure_file['iacuc_protocol'],
                    animal_weight_prior=procedure_file['animal_weight_prior'],
                    animal_weight_post=procedure_file['animal_weight_post'],
                    weight_unit=procedure_file['weight_unit'],
                    anaesthesia=procedure_file['anaesthesia'],
                    workstation_id=procedure_file['workstation_id'],
                    procedures=procedures,
                    notes=procedure_file['notes'],
                )
            )



        
    def matched_value(self, value, key, row):
        if row[key] != value and value != None:
            logging.warning(f"Value mismatch for {key}:\nspreadsheet value: {value}\nprocedure value: {row[key]}.")
            return False
        return True

    def run_job(self) -> JobResponse:
        """Run the job and return the response."""

        extracted = self._extract()
        transformed = self._transform(extracted)

        job_response = self._load(
            transformed, self.job_settings.output_directory
        )

        return job_response