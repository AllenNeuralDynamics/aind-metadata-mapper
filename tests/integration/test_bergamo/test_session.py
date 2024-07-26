"""Integration test for bergamo session"""

from src.aind_metadata_mapper.bergamo.session import JobSettings, BergamoEtl
import s3fs
import os


INPUT_SOURCE = os.getenv(TEST_DATA_DIR)

def test_bergamo_etl():
    experimenter_full_name = "Miles Morales"
    subject_id = "706957"

    job_settings = JobSettings(
        input_source=INPUT_SOURCE,
        experimenter_full_name=[experimenter_full_name],
        subject_id=subject_id,
        imaging_laser_wavelength=12,
        fov_imaging_depth=12,
        fov_targeted_structure="some structure",
        notes=None,
    )
    bergamo_job = BergamoEtl(job_settings=job_settings)
    response = bergamo_job.run_job()

