"""Integration test for bergamo session"""

from src.aind_metadata_mapper.bergamo.session import JobSettings, BergamoEtl
import s3fs
import os

BUCKET_NAME = os.getenv("AWS_DATA_SCHEMA_BUCKET")
BASE_DIR = "metadata-mapper-integration-testing/"
BERGAMO_INPUT_SOURCE = f"{BUCKET_NAME}/{BASE_DIR}/single-plane-ophys_706957_2024-01-12_16-13-29/ophys/"
LOCAL_DIR = "resources/bergamo/tiff_files/"


def main():
    experimenter_full_name = "Miles Morales"
    subject_id = "706957"

    s3fs_client = s3fs.core.S3FileSystem()
    s3_ls = s3fs_client.ls(BERGAMO_INPUT_SOURCE)

    # Ensure the local directory exists
    os.makedirs("resources/bergamo/tiff_files/", exist_ok=True)

    for path in s3_ls:
        if s3fs_client.isdir(path):
            continue

        # Construct local file path
        relative_path = os.path.relpath(path, BERGAMO_INPUT_SOURCE)
        local_file_path = os.path.join(LOCAL_DIR, relative_path)

        # Ensure local subdirectories exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        # Download the file
        s3fs_client.get(path, local_file_path)

    job_settings = JobSettings(
        input_source=LOCAL_DIR,
        experimenter_full_name=[experimenter_full_name],
        subject_id=subject_id,
        imaging_laser_wavelength=12,
        fov_imaging_depth=12,
        fov_targeted_structure="some structure",
        notes=None,
    )
    bergamo_job = BergamoEtl(job_settings=job_settings)
    response = bergamo_job.run_job()
    print(response)


if __name__ == "__main__":
    main()
