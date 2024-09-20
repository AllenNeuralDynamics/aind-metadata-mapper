"""Test U19 ETL class."""

import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from aind_data_schema.core.procedures import (
    Procedures,
    Reagent,
    SpecimenProcedure,
    SpecimenProcedureType,
)
from aind_data_schema_models.organizations import Organization

from aind_metadata_mapper.u19.models import JobSettings
from aind_metadata_mapper.u19.procedures import (
    U19Etl,
    get_dates,
    strings_to_dates,
)
from aind_metadata_mapper.u19.utils import construct_new_model

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "u19"
)

EXAMPLE_TISSUE_SHEET = RESOURCES_DIR / "example_tissue_subject.xlsx"
EXAMPLE_DOWNLOAD_PROCEDURE = (
    RESOURCES_DIR / "example_downloaded_procedure.json"
)
EXAMPLE_DOWNLOAD_RESPONSE = RESOURCES_DIR / "example_downloaded_response.json"
EXAMPLE_OUTPUT = RESOURCES_DIR / "example_output.json"


class TestU19Writer(unittest.TestCase):
    """Test methods in SchemaWriter class."""

    @classmethod
    def setUpClass(self):
        """Set up class for testing."""

        with open(EXAMPLE_OUTPUT, "r") as f:
            self.example_output = json.load(f)

        self.example_job_settings = JobSettings(
            input_source=EXAMPLE_TISSUE_SHEET,
            tissue_sheet_names=[
                "example_sheet",
                "extra sheet",
            ],
            experimenter_full_name=["Some Fella"],
            procedures_download_link="fake_download_link",
            subject_to_ingest="721832",
            allow_validation_errors=True,
        )

    @patch(
        "aind_metadata_mapper.u19.procedures.U19Etl.download_procedure_file"
    )
    def test_run_job(self, mock_download_procedure):
        """Test run_job method."""

        with open(EXAMPLE_DOWNLOAD_PROCEDURE, "r") as f:
            mock_download_procedure.return_value = json.load(f)

        etl = U19Etl(self.example_job_settings)
        job_response = etl.run_job()

        actual_output = json.loads(job_response.data)

        self.assertEqual(self.example_output, actual_output)

    @patch(
        "aind_metadata_mapper.u19.procedures.U19Etl.download_procedure_file"
    )
    def test_extract(self, mock_download_procedure):
        """Test extract method."""

        with open(EXAMPLE_DOWNLOAD_PROCEDURE, "r") as f:
            mock_download_procedure.return_value = json.load(f)

        etl = U19Etl(self.example_job_settings)
        extracted = etl._extract(self.example_job_settings.subject_to_ingest)

        self.assertEqual(
            extracted["subject_id"],
            self.example_job_settings.subject_to_ingest,
        )

    def test_transform(self):
        """Test transform method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()

        with open(EXAMPLE_DOWNLOAD_PROCEDURE, "r") as f:
            extracted = json.load(f)

        transformed = etl._transform(
            extracted, self.example_job_settings.subject_to_ingest
        )

        self.assertEqual(
            len(transformed.specimen_procedures),
            len(
                construct_new_model(
                    self.example_output, Procedures, True
                ).specimen_procedures
            ),
        )

    @patch("aind_metadata_mapper.u19.procedures.U19Etl._transform")
    def test_load(self, mock_transform):
        """Test load method."""

        mock_transform.return_value = construct_new_model(
            self.example_output, Procedures, True
        )

        etl = U19Etl(self.example_job_settings)
        transformed = etl._transform(
            self.example_job_settings.subject_to_ingest
        )

        job_response = etl._load(
            transformed, self.example_job_settings.output_directory
        )

        actual_output = json.loads(job_response.data)

        self.assertEqual(self.example_output, actual_output)

    def test_find_sheet_row(self):
        """Test find_sheet_row method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()
        row = etl.find_sheet_row(self.example_job_settings.subject_to_ingest)

        self.assertTrue(row is not None)

    @patch("requests.get")
    def test_download_procedure_file(self, mock_requests):
        """Test download_procedure_file method."""

        with open(EXAMPLE_DOWNLOAD_RESPONSE, "r") as f:
            example_download_response = json.load(f)
            mock_requests.return_value.json.return_value = (
                example_download_response
            )
            mock_requests.return_value.status_code = 200

        etl = U19Etl(self.example_job_settings)
        response = etl.download_procedure_file(
            self.example_job_settings.subject_to_ingest
        )

        print(f"TEST DATA: {response}")
        print(f"EXAMPLE DATA: {example_download_response['data']}")

        self.assertEqual(response, example_download_response["data"])

    def test_load_specimen_procedure_file(self):
        """Test load_specimen_procedure_file method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()

        self.assertTrue(len(etl.tissue_sheets) == 2)

    def test_strings_to_dates(self):
        """Test strings_to_dates method."""

        date_str = "12/01/22 - 12/02/22"
        dates = get_dates(date_str)
        dates = strings_to_dates(dates)

        self.assertEqual(
            dates[0], datetime.strptime("12/01/22", "%m/%d/%y").date()
        )
        self.assertEqual(
            dates[1], datetime.strptime("12/02/22", "%m/%d/%y").date()
        )

    def test_extract_spec_procedures(self):
        """Test extract_spec_procedures method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()

        row = etl.find_sheet_row(self.example_job_settings.subject_to_ingest)

        easyindex_100_date = row["Index matching"]["100% EasyIndex"][
            "Date(s)"
        ].iloc[0]
        if not pd.isna(easyindex_100_date):
            easyindex_100_date = strings_to_dates(
                get_dates(easyindex_100_date)
            )

        easyindex_100_lot = row["Index matching"]["EasyIndex"]["Lot#"].iloc[0]
        if pd.isna(easyindex_100_lot):
            easyindex_100_lot = "unknown"

        easyindex_notes = row["Index matching"]["Notes"][
            "Unnamed: 22_level_2"
        ].iloc[0]
        if pd.isna(easyindex_notes):
            easyindex_notes = "None"

        easyindex_100_reagent = Reagent(
            name="EasyIndex",
            source=Organization.LIFECANVAS,
            lot_number=easyindex_100_lot,
        )

        test_spec_procedure = SpecimenProcedure(
            specimen_id=self.example_job_settings.subject_to_ingest,
            procedure_type=SpecimenProcedureType.REFRACTIVE_INDEX_MATCHING,
            procedure_name="100% EasyIndex",
            start_date=easyindex_100_date[0],
            end_date=easyindex_100_date[1],
            experimenter_full_name="Some Fella",
            protocol_id=["none"],
            reagents=[easyindex_100_reagent],
            notes=easyindex_notes,
        )

        extracted_procedures = etl.extract_spec_procedures(
            self.example_job_settings.subject_to_ingest, row
        )

        self.assertEqual(len(extracted_procedures), 6)
        self.assertEqual(extracted_procedures[5], test_spec_procedure)


if __name__ == "__main__":
    unittest.main()
