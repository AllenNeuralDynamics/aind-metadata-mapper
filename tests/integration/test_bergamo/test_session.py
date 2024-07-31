"""Integration test for bergamo session"""

from aind_metadata_mapper.bergamo.session import JobSettings, BergamoEtl
import s3fs
import os
import json
import shutil

BUCKET_NAME = os.getenv("AWS_METADATA_MAPPER_BUCKET")
BASE_DIR = "metadata-mapper-integration-testing/bergamo"
BERGAMO_INPUT_SOURCE = f"{BUCKET_NAME}/{BASE_DIR}/tiff_files/"
EXPECTED_SESSION = f"{BUCKET_NAME}/{BASE_DIR}/expected_session.json"
LOCAL_DIR = "resources/bergamo/tiff_files/"


def test_bergamo_etl():
    """Tests that BergamoETL creates session as expected."""

    s3fs_client = s3fs.core.S3FileSystem()
    s3_ls = s3fs_client.ls(BERGAMO_INPUT_SOURCE)

    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Copies TIFF files from s3 source
    for path in s3_ls:
        if not s3fs_client.isdir(path):
            relative_path = os.path.relpath(path, BERGAMO_INPUT_SOURCE)
            local_file_path = os.path.join(LOCAL_DIR, relative_path)

            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            s3fs_client.get(path, local_file_path)

    # Define JobSettings and run Bergamo job
    job_settings = JobSettings(
        input_source=LOCAL_DIR,
        experimenter_full_name=["Jane Doe"],
        subject_id="706957",
        imaging_laser_wavelength=405,
        fov_imaging_depth=150,
        fov_targeted_structure="M1",
        notes=None,
    )
    bergamo_job = BergamoEtl(job_settings=job_settings)
    response = bergamo_job.run_job()

    with s3fs_client.open(EXPECTED_SESSION, "r") as s3_file:
        expected_session = json.load(s3_file)

    assert json.loads(response.data) == expected_session

    # Delete downloaded content
    shutil.rmtree(LOCAL_DIR)
    print("Cleanup complete: All downloaded files have been deleted.")


if __name__ == "__main__":
    test_bergamo_etl()
