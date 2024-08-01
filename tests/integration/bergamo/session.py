"""Integration test for bergamo session"""

import argparse
import json
import os
import sys
import unittest
from pathlib import Path

from aind_metadata_mapper.bergamo.session import BergamoEtl, JobSettings

EXPECTED_OUTPUT_FILE_PATH = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / ".."
    / "resources"
    / "bergamo"
    / "session.json"
)


class IntegrationTestBergamo(unittest.TestCase):
    """Integration test for Bergamo"""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the class."""
        with open(EXPECTED_OUTPUT_FILE_PATH, "r") as f:
            expected_output_json = json.load(f)
        cls.expected_output = expected_output_json

    def test_run_job(self):
        """Tests run_job on actual raw data source."""
        input_source: str = getattr(IntegrationTestBergamo, "input_source")
        job_settings = JobSettings(
            input_source=Path(input_source),
            experimenter_full_name=["Jane Doe"],
            subject_id="706957",
            imaging_laser_wavelength=405,
            fov_imaging_depth=150,
            fov_targeted_structure="M1",
            notes=None,
        )
        bergamo_job = BergamoEtl(job_settings=job_settings)
        response = bergamo_job.run_job()
        actual_session = json.loads(response.data)
        self.assertEqual(self.expected_output, actual_session)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_source",
        type=str,
        required=True,
        help="The input source for the ETL job.",
    )
    parser.add_argument("unittest_args", nargs="*")

    args = parser.parse_args()
    setattr(IntegrationTestBergamo, "input_source", args.input_source)
    sys.argv[1:] = args.unittest_args
    unittest.main()
