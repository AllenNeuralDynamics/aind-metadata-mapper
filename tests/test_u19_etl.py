import json
import os
import pickle
import unittest
from pathlib import Path
from unittest.mock import patch
from aind_metadata_mapper.U19.u19_etl import JobSettings, U19Etl

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "U19"
)

EXAMPLE_TISSUE_SHEET = RESOURCES_DIR / "example_tissue_sheet.xlsx"

class TestU19Writer(unittest.TestCase):
    """Test methods in SchemaWriter class."""

    def setUpClass(cls):
        """Set up class for testing."""

        cls.example_output = RESOURCES_DIR / "example_output.json"

        cls.example_job_settings = JobSettings(
            tissue_sheet_path= EXAMPLE_TISSUE_SHEET,
            tissue_sheet_names=[
                "Dec 2022 - Feb 2023",
                "Mar - May 2023",
                "Jun - Aug 2023",
                "Sep - Nov 2023",
                "Dec 2023 - Feb 2024",
                "Mar - May 2024",
            ],
            output_directory = Path(f"./output"),
            experimenter_full_name = ["Mathew Summers"],
            subject_to_ingest = '721832',
            allow_validation_errors = True
        )

    def test_run_job(self):
        """Test run_job method."""
        etl = U19Etl(self.example_job_settings)
        job_response = etl.run_job()

        with open(self.example_output, "r") as f:
            expected_output = json.load(f)

        actual_output = json.loads(job_response.data)

        self.assertEqual(expected_output, actual_output)

    