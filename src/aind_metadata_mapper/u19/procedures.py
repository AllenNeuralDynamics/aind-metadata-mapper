"""Sets up the U19 ingest ETL"""

import json
import logging
import sys
from datetime import datetime
from typing import Union

import pandas as pd
import requests
from aind_data_schema.core.procedures import (
    Procedures,
    Reagent,
    SpecimenProcedure,
    SpecimenProcedureType,
)
from aind_data_schema_models.organizations import Organization

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.u19.models import JobSettings
from aind_metadata_mapper.u19.utils import construct_new_model


def get_dates(string):
    """Get the dates from a string."""
    return [str.strip(date) for date in string.split(" - ")]


def strings_to_dates(strings):
    """Convert strings to dates."""
    date1 = datetime.strptime(strings[0], "%m/%d/%y").date()
    date2 = datetime.strptime(strings[1], "%m/%d/%y").date()
    return [date1, date2]


class SmartSPIMSpecimenIngester(GenericEtl[JobSettings]):
    """U19 ETL class."""

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
            job_settings_model.tissue_sheet_path is not None
            and job_settings_model.input_source is None
        ):
            job_settings_model.input_source = (
                job_settings_model.tissue_sheet_path
            )
        super().__init__(job_settings=job_settings_model)

    def run_job(self) -> JobResponse:
        """Run the job and return the response."""

        extracted = self._extract(self.job_settings.subject_to_ingest)
        if type(extracted) is JobResponse:
            return extracted
        transformed = self._transform(
            extracted, self.job_settings.subject_to_ingest
        )

        job_response = self._load(
            transformed, self.job_settings.output_directory
        )
        return job_response

    def _extract(self, subj):
        """Extract the data from the U19 server."""
        self.load_specimen_procedure_file()

        logging.info(f"Extracting subj: {subj}")
        existing_procedure = self.download_procedure_file(subj)

        return existing_procedure

    def _transform(self, existing_procedure, subj_id):
        """Transform the data into the correct format."""

        if existing_procedure is not None:
            row = self.find_sheet_row(subj_id)
            if row is None:
                logging.warning(f"Could not find row for {subj_id}")
                return
            existing_procedure["specimen_procedures"] = (
                self.extract_spec_procedures(subj_id, row)
            )

            return construct_new_model(
                existing_procedure,
                Procedures,
                self.job_settings.allow_validation_errors,
            )

    def find_sheet_row(self, subj_id):
        """Return the sheet that the subject is on."""
        for sheet in self.tissue_sheets:
            if (
                int(subj_id)
                in sheet["SubjInfo"]["Unnamed: 0_level_1"]["Mouse ID"].tolist()
            ):
                return sheet.loc[
                    sheet["SubjInfo"]["Unnamed: 0_level_1"]["Mouse ID"]
                    == int(subj_id)
                ]

    def download_procedure_file(self, subj_id: str):
        """Download the procedure file for a subject."""
        # Get the procedure file from the U19 server
        request = requests.get(
            f"{self.job_settings.procedures_download_link}/{subj_id}"
        )

        logging.info(
            f"Downloaded {subj_id} model with "
            f"status code: {request.status_code}"
        )

        if request.status_code in [404, 500, 503, 422]:
            logging.error(f"{subj_id} model not found")
            return JobResponse(
                status_code=request.status_code,
                message=f"Procedures model not found for {subj_id}",
                data=None,
            )

        try:
            item = request.json()
        except json.JSONDecodeError:
            logging.error(f"Error decoding json for {subj_id}: {request.text}")
            return JobResponse(
                status_code=request.status_code,
                message=f"Error decoding json for {subj_id}",
                data=None,
            )

        if request.status_code == 200:
            return item["data"]
        elif request.status_code in [207, 406]:
            logging.warning(f"Validation errors for {subj_id}")
            return item["data"]

        logging.error(
            f"Unknown error while downloading procedures for {subj_id}"
        )
        return JobResponse(
            status_code=request.status_code,
            message="Unknown error while downloading "
            f"procedures for {subj_id}",
            data=None,
        )

    def load_specimen_procedure_file(self):
        """Load the specimen procedure file."""

        self.tissue_sheets = []

        for sheet_name in self.job_settings.tissue_sheet_names:
            df = pd.read_excel(
                self.job_settings.input_source,
                sheet_name=sheet_name,
                header=[0, 1, 2],
            )
            self.tissue_sheets.append(df)

    def extract_spec_procedures(self, subj_id, row):  # noqa: C901
        """Extract the specimen procedures from the spreadsheet."""

        default_source = Organization.LIFECANVAS

        subj_id = (
            str(row["SubjInfo"]["Unnamed: 0_level_1"]["Mouse ID"].iloc[0])
            .strip()
            .lower()
        )

        experimenter = row["SubjInfo"]["Unnamed: 2_level_1"][
            "Experimenter"
        ].iloc[0]

        shield_off_date = row["Fixation"]["SHIELD OFF"]["Date(s)"].iloc[0]

        if not pd.isna(shield_off_date):
            shield_off_start_date, shield_off_end_date = strings_to_dates(
                get_dates(shield_off_date)
            )
        shield_buffer_lot = row["Fixation"]["SHIELD Buffer"]["Lot#"].iloc[0]
        if pd.isna(shield_buffer_lot):
            shield_buffer_lot = "unknown"
        shield_epoxy_lot = row["Fixation"]["SHIELD Epoxy"]["Lot#"].iloc[0]
        if pd.isna(shield_epoxy_lot):
            shield_epoxy_lot = "unknown"

        shield_buffer_reagent = Reagent(
            name="SHIELD Buffer",
            source=default_source,
            lot_number=shield_buffer_lot,
        )

        shield_epoxy_reagent = Reagent(
            name="SHIELD Epoxy",
            source=default_source,
            lot_number=shield_epoxy_lot,
        )

        shield_on_date = row["Fixation"]["SHIELD ON"]["Date(s)"].iloc[0]
        if not pd.isna(shield_on_date):
            shield_on_start_date, shield_on_end_date = strings_to_dates(
                get_dates(shield_on_date)
            )
        shield_on_lot = row["Fixation"]["SHIELD ON"]["Lot#"].iloc[0]
        if pd.isna(shield_on_lot):
            shield_on_lot = "unknown"
        fixation_notes = row["Fixation"]["Notes"]["Unnamed: 9_level_2"].iloc[0]
        if pd.isna(fixation_notes):
            fixation_notes = "None"

        shield_on_reagent = Reagent(
            name="SHIELD ON", source=default_source, lot_number=shield_on_lot
        )

        passive_delipidation_dates = row["Passive delipidation"][
            "24 Hr Delipidation "
        ]["Date(s)"].iloc[0]
        if not pd.isna(passive_delipidation_dates):
            (
                passive_delipidation_start_date,
                passive_delipidation_end_date,
            ) = strings_to_dates(get_dates(passive_delipidation_dates))
        passive_conduction_buffer_lot = row["Passive delipidation"][
            "Delipidation Buffer"
        ]["Lot#"].iloc[0]
        if pd.isna(passive_conduction_buffer_lot):
            passive_conduction_buffer_lot = "unknown"
        passive_delip_notes = row["Passive delipidation"]["Notes"][
            "Unnamed: 12_level_2"
        ].iloc[0]
        passive_delip_source = default_source
        if not pd.isna(passive_delip_notes):
            if (
                "SBiP" in passive_delip_notes
                or "dicholoromethane" in passive_delip_notes
            ):
                passive_delip_source = Organization.SIGMA
        else:
            passive_delip_notes = "None"

        passive_delip_reagent = Reagent(
            name="Delipidation Buffer",
            source=passive_delip_source,
            lot_number=passive_conduction_buffer_lot,
        )

        active_delipidation_dates = row["Active Delipidation"][
            "Active Delipidation"
        ]["Date(s)"].iloc[0]
        if not pd.isna(active_delipidation_dates):
            active_delip_start_date, active_delip_end_date = strings_to_dates(
                get_dates(active_delipidation_dates)
            )
        active_conduction_buffer_lot = row["Active Delipidation"][
            "Conduction Buffer"
        ]["Lot#"].iloc[0]
        if pd.isna(active_conduction_buffer_lot):
            active_conduction_buffer_lot = "unknown"

        active_delip_notes = row["Active Delipidation"]["Notes"][
            "Unnamed: 17_level_2"
        ].iloc[0]
        if pd.isna(active_delip_notes):
            active_delip_notes = "None"

        active_delip_reagent = Reagent(
            name="Conduction Buffer",
            source=default_source,
            lot_number=active_conduction_buffer_lot,
        )

        easyindex_50_date = row["Index matching"]["50% EasyIndex"][
            "Date(s)"
        ].iloc[0]
        if not pd.isna(easyindex_50_date):
            easyindex_50_start_date, easyindex_50_end_date = strings_to_dates(
                get_dates(easyindex_50_date)
            )
        easyindex_50_lot = row["Index matching"]["EasyIndex"]["Lot#"].iloc[0]
        if pd.isna(easyindex_50_lot):
            easyindex_50_lot = "unknown"
        easyindex_100_date = row["Index matching"]["100% EasyIndex"][
            "Date(s)"
        ].iloc[0]
        if not pd.isna(easyindex_100_date):
            (
                easyindex_100_start_date,
                easyindex_100_end_date,
            ) = strings_to_dates(get_dates(easyindex_100_date))
        easyindex_100_lot = row["Index matching"]["EasyIndex"]["Lot#"].iloc[0]
        if pd.isna(easyindex_100_lot):
            easyindex_100_lot = "unknown"
        easyindex_notes = row["Index matching"]["Notes"][
            "Unnamed: 22_level_2"
        ].iloc[0]
        if pd.isna(easyindex_notes):
            easyindex_notes = "None"

        easyindex_50_reagent = Reagent(
            name="EasyIndex",
            source=passive_delip_source,
            lot_number=easyindex_50_lot,
        )

        easyindex_100_reagent = Reagent(
            name="EasyIndex",
            source=passive_delip_source,
            lot_number=easyindex_100_lot,
        )

        overall_notes = row["Index matching"]["Notes"][
            "Unnamed: 24_level_2"
        ].iloc[0]
        if pd.isna(overall_notes):
            overall_notes = None

        items = []

        if not pd.isna(shield_off_date):
            shield_off_procedure = SpecimenProcedure(
                specimen_id=subj_id,
                procedure_type=SpecimenProcedureType.FIXATION,
                procedure_name="SHIELD OFF",
                start_date=shield_off_start_date,
                end_date=shield_off_end_date,
                experimenter_full_name=experimenter,
                protocol_id=["none"],
                reagents=[shield_epoxy_reagent, shield_buffer_reagent],
                notes=fixation_notes,
            )
            items.append(shield_off_procedure)

        if not pd.isna(shield_on_date):
            shield_on_procedure = SpecimenProcedure(
                specimen_id=subj_id,
                procedure_type=SpecimenProcedureType.FIXATION,
                procedure_name="SHIELD ON",
                start_date=shield_on_start_date,
                end_date=shield_on_end_date,
                experimenter_full_name=experimenter,
                protocol_id=["none"],
                reagents=[shield_on_reagent],
                notes=fixation_notes,
            )
            items.append(shield_on_procedure)

        if not pd.isna(passive_delipidation_dates):
            passive_delip_procedure = SpecimenProcedure(
                specimen_id=subj_id,
                procedure_type=SpecimenProcedureType.DELIPIDATION,
                procedure_name="24h Delipidation",
                start_date=passive_delipidation_start_date,
                end_date=passive_delipidation_end_date,
                experimenter_full_name=experimenter,
                protocol_id=["none"],
                reagents=[passive_delip_reagent],
                notes=passive_delip_notes,
            )
            items.append(passive_delip_procedure)

        if not pd.isna(active_delipidation_dates):
            active_delip_procedure = SpecimenProcedure(
                specimen_id=subj_id,
                procedure_type=SpecimenProcedureType.DELIPIDATION,
                procedure_name="Active Delipidation",
                start_date=active_delip_start_date,
                end_date=active_delip_end_date,
                experimenter_full_name=experimenter,
                protocol_id=["none"],
                reagents=[active_delip_reagent],
                notes=active_delip_notes,
            )
            items.append(active_delip_procedure)

        if not pd.isna(easyindex_50_date):
            easyindex_50_procedure = SpecimenProcedure(
                specimen_id=subj_id,
                procedure_type=SpecimenProcedureType.REFRACTIVE_INDEX_MATCHING,
                procedure_name="50% EasyIndex",
                start_date=easyindex_50_start_date,
                end_date=easyindex_50_end_date,
                experimenter_full_name=experimenter,
                protocol_id=["none"],
                reagents=[easyindex_50_reagent],
                notes=easyindex_notes,
            )
            items.append(easyindex_50_procedure)

        if not pd.isna(easyindex_100_date):
            easyindex_100_procedure = SpecimenProcedure(
                specimen_id=subj_id,
                procedure_type=SpecimenProcedureType.REFRACTIVE_INDEX_MATCHING,
                procedure_name="100% EasyIndex",
                start_date=easyindex_100_start_date,
                end_date=easyindex_100_end_date,
                experimenter_full_name=experimenter,
                protocol_id=["none"],
                reagents=[easyindex_100_reagent],
                notes=easyindex_notes,
            )
            items.append(easyindex_100_procedure)

        return items


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = SmartSPIMSpecimenIngester(job_settings=main_job_settings)
    etl.run_job()
