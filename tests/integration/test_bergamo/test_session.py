"""Integration test for bergamo session"""

from aind_metadata_mapper.bergamo.session import JobSettings, BergamoEtl
import sys
import json
import os
from pathlib import Path
import argparse

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / ".."
    / "resources"
    / "bergamo"
)


def parse_arguments():
    """Parse input source from command-line argument"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_source',
        type=str,
        required=True,
        help="The input source for the ETL job.")

    return parser.parse_args()


def run_integration_test(sys_args):
    """Tests that BergamoETL creates session as expected."""
    input_source = sys_args.input_source
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

    with open(f"{RESOURCES_DIR}/session.json", "r") as file:
        expected_session = json.load(file)

    assert json.loads(response.data) == expected_session


if __name__ == "__main__":
    sys_args = parse_arguments()
    run_integration_test(sys_args=sys_args)
