User Guide
==========
Thank you for using ``aind-metadata-mapper``! This guide is intended for scientists and engineers in AIND that wish to generate metadata models (particularly the session model) from specific acquisition machines.

Metadata Architecture
----------------------
This repository is split up into different packages by acquisition machine. Each package defines a ``JobSettings`` object and an ``ETL`` to generate a desired model.
Each JobSettings defines the information necessary to compile metadata, and varies by machine. For example, a bergamo session model can be generated directly like so:

.. code:: python

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

However, it is also possible to generate the session.json as part of the ``aind-data-transfer-service``'s process of uploading data to the cloud.

This will be done through the basic flow:
    - A user submits a request to upload a session. As part of the request, information about how to compile the metadata will be added.
    - Airflow parses the configs and submits requests to SLURM to compile the metadata and save it to a staging directory in VAST together with the modality data.
    - Once the data is gathered, airflow sends a request to SLURM to upload the staging folder to S3

The information will be passed via field ``session_settings``. This field supports:
    - Using a JobSettings json file that can be read in the metadata mapper
    - Defining the pydantic model and passing those configs directly

Here's a diagram of the overall architecture:

.. image:: ../diagrams/metadata_pipeline.png
   :alt: Metadata Workflow
   :width: 400px
   :align: center

The rest of this document will describe how to generate the session in each of these ways.

Components
~~~~~~~~~~
    - **AWS S3**: cloud object storage holding acquired files and metadata in JSON format.
    - **Airflow**: workflow management platform for data engineering pipelines.
    - **SLURM**: job scheduler for Linux and Unix-like kernels.
    - **VAST**: a temporary storage buffer for data.


Using a JobSettings json file
-----------------------------
This workflow assumes that the user has a pre-defined JobSettings defined in a json file. The example below generates and uploads a session.json with a pre-defined config file that is on VAST.

.. code:: python

    import json
    import requests

    from aind_data_transfer_models.core import (
        ModalityConfigs,
        BasicUploadJobConfigs,
        SubmitJobRequest,
    )
    from aind_data_schema_models.modalities import Modality
    from aind_data_schema_models.platforms import Platform
    from datetime import datetime
    import warnings

    acq_datetime = datetime.fromisoformat("2000-01-01T01:11:32")

    # 1. Define metadata configuration using a pre-existing settings file
    metadata_configs_from_file = {
        "session_settings": {
            "job_settings": {
                "user_settings_config_file":"/allen/aind/scratch/svc_aind_upload/test_data_sets/bci/test_bergamo_settings.json",
                "job_settings_name": "Bergamo"
            }
        }
    }

    # 2. Define necessary fields for UploadJob
    ephys_config = ModalityConfigs(
        modality=Modality.ECEPHYS,
        source=(
            "/allen/aind/scratch/svc_aind_upload/test_data_sets/ecephys/655019_2023-04-03_18-17-07"
        ),
    )
    project_name = "Ephys Platform"
    subject_id = "655019"
    platform = Platform.ECEPHYS
    s3_bucket = "private"

    # 3. Define UploadJobConfigs. Fill in 'metadata_configs' to generate and upload session
    upload_job_configs = BasicUploadJobConfigs(
        project_name=project_name,
        s3_bucket=s3_bucket,
        platform=platform,
        subject_id=subject_id,
        acq_datetime=acq_datetime,
        modalities=[ephys_config],
        metadata_configs=metadata_configs_from_file,
    )

    upload_jobs = [upload_job_configs]


    # Because we use a dict, this may raise a serializer warning.
    # The warning can be suppressed with
    with warnings.catch_warnings():
      warnings.simplefilter("ignore", UserWarning)
      submit_request = SubmitJobRequest(
          upload_jobs=upload_jobs
      )

    post_request_content = json.loads(submit_request.model_dump_json(round_trip=True, exclude_none=True, warnings=False))
    submit_job_response = requests.post(url="http://aind-data-transfer-service-dev/api/v1/submit_jobs", json=post_request_content)

Defining and passing JobSettings directly
-----------------------------------------
This example demonstrates how to define ``JobSettings`` and generate the session.json through the GatherMetadataJob.

.. code:: python

    import json
    import requests

    from aind_data_transfer_models.core import (
        ModalityConfigs,
        BasicUploadJobConfigs,
        SubmitJobRequest,
    )
    from aind_metadata_mapper.models import SessionSettings, JobSettings as GatherMetadataJobSettings
    from aind_metadata_mapper.bergamo.models import JobSettings as BergamoSessionSettings
    from aind_data_schema_models.modalities import Modality
    from aind_data_schema_models.platforms import Platform
    from datetime import datetime

    acq_datetime = datetime.fromisoformat("2000-01-01T01:11:33")

    # 1. Define the JobSettings for desired acquisition machine
    bergamo_session_settings = BergamoSessionSettings(
                input_source="/allen/aind/scratch/svc_aind_upload/test_data_sets/bci/061022",
                experimenter_full_name=["John Apple"],
                subject_id="655019",
                imaging_laser_wavelength=920,
                fov_imaging_depth=200,
                fov_targeted_structure="Primary Motor Cortex",
                notes="test upload",
    )

    # 2. Define SessionSettings object with defined job settings
    session_settings = SessionSettings(job_settings=bergamo_session_settings)

    # directory_to_write_to is required, but will be set later.
    # We can set it to "stage" for now.
    # 3. Define GatherMetadataJobSettings with session_settings. Note that you can define settings for different metadata files here
    metadata_job_settings = GatherMetadataJobSettings(directory_to_write_to="stage", session_settings=session_settings)

    # 4. Define necessary fields for UploadJob
    ephys_config = ModalityConfigs(
        modality=Modality.ECEPHYS,
        source=(
            "/allen/aind/scratch/svc_aind_upload/test_data_sets/ecephys/655019_2023-04-03_18-17-07"
        ),
    )
    project_name = "Ephys Platform"
    subject_id = "655019"
    platform = Platform.ECEPHYS
    s3_bucket = "private"

    # 5. Define UploadJobConfigs. Fill in 'metadata_configs' to generate and upload session
    upload_job_configs = BasicUploadJobConfigs(
        project_name=project_name,
        s3_bucket=s3_bucket,
        platform=platform,
        subject_id=subject_id,
        acq_datetime=acq_datetime,
        modalities=[ephys_config],
        metadata_configs=metadata_job_settings,
    )

    upload_jobs = [upload_job_configs]

    # 6. Submit and post request
    submit_request = SubmitJobRequest(
        upload_jobs=upload_jobs
    )


    post_request_content = json.loads(submit_request.model_dump_json(round_trip=True, exclude_none=True))
    submit_job_response = requests.post(url="http://aind-data-transfer-service-dev/api/v1/submit_jobs", json=post_request_content)

Viewing the status of submitted jobs
------------------------------------
The status of submitted jobs can be viewed at: http://aind-data-transfer-service/jobs

Reporting bugs or making feature requests
-----------------------------------------
Please report any bugs or feature requests here: `issues <https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues>`_