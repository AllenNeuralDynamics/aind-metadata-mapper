import json
import os
import pickle
import unittest
from pathlib import Path
from unittest.mock import patch
from aind_metadata_mapper.U19.u19_etl import JobSettings, U19Etl
from aind_metadata_upgrader.utils import construct_new_model
from aind_data_schema.core.procedures import Procedures

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / "resources"
    / "U19"
)

EXAMPLE_TISSUE_SHEET = RESOURCES_DIR / "example_tissue_sheet.xlsx"
EXAMPLE_DOWNLOAD_PROCEDURE = RESOURCES_DIR / "example_downloaded_procedure.json"
EXAMPLE_OUTPUT = RESOURCES_DIR / "example_output.json"

class TestU19Writer(unittest.TestCase):
    """Test methods in SchemaWriter class."""

    @classmethod
    def setUpClass(self):
        """Set up class for testing."""

        with open(EXAMPLE_OUTPUT, "r") as f:
            self.example_output = json.load(f)

        self.example_job_settings = JobSettings(
            tissue_sheet_path= EXAMPLE_TISSUE_SHEET,
            tissue_sheet_names=[
                "Dec 2022 - Feb 2023",
                "Mar - May 2023",
                "Jun - Aug 2023",
                "Sep - Nov 2023",
                "Dec 2023 - Feb 2024",
                "Mar - May 2024",
            ],
            experimenter_full_name = ["Mathew Summers"],
            subject_to_ingest = '721832',
            allow_validation_errors = True
        )

    def test_constructor_from_string(self) -> None:
        """Test constructor from string."""

        job_settings_string = self.example_job_settings.model_dump_json()
        etl0 = U19Etl(self.example_job_settings)
        etl1 = U19Etl(job_settings_string)

        self.assertEqual(etl1.job_settings, etl0.job_settings)

    def test_run_job(self):
        """Test run_job method."""
        etl = U19Etl(self.example_job_settings)
        job_response = etl.run_job()

        actual_output = json.loads(job_response.data)

        self.assertEqual(self.example_output, actual_output)

    def test_load_specimen_file(self):
        """Test load_specimen_file method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()

        self.assertTrue(len(etl.tissue_sheets) == 6)

    def test_load_specimen_procedure_file(self):
        """Test extract_spec_procedures method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()

        self.assertTrue(len(etl.tissue_sheets) == 6)

    def test_find_sheet_row(self):
        """Test find_sheet_row method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()
        row = etl.find_sheet_row("721832")

        self.assertTrue(row is not None)
    
    def test_extract(self):
        """Test extract method."""

        etl = U19Etl(self.example_job_settings)
        extracted = etl._extract("721832")

        self.assertEqual(extracted["subject_id"], "721832")

    def test_transform(self):
        """Test transform method."""

        etl = U19Etl(self.example_job_settings)
        etl.load_specimen_procedure_file()

        with open(EXAMPLE_DOWNLOAD_PROCEDURE, "r") as f:
            extracted = json.load(f)

        transformed = etl._transform(extracted, "721832")

        self.assertEqual(transformed, construct_new_model(self.example_output, Procedures, True))
