User Guide
==========
Thank you for using ``aind-metadata-mapper``! This guide is intended for scientists and engineers in AIND that wish to generate metadata models (particularly the session model) from specific acquisition machines.
This repository is designed to centralize code to ``Extract``, ``Transform``, and ``Load`` data from multiple acquisition machines. 

1. Installation
----------------
Because the metadata-mapper supports multiple acquisition machines and data files, there are large and acquisition-specific dependencies.
To mitigate this, we recommend installing only the packages you need. 

For example, if you are using the Bergamo acquisition machine and would like to use this package to generate a session.json after an experiment, you can install the package like so:

.. code:: bash

    pip install aind-metadata-mapper[bergamo]

The list of acquisition-specific packages is as follows:

- ``bergamo``: Bergamo acquisition machine
- ``openephys``: Ephys acquisition machine
- ``ephys``: Ephys acquisition machine (deprecated, use openephys instead)
- ``bruker``: BCI acquisition machine
- ``mesoscope``: Mesoscope acquisition machine
- ``u19``: U19 acquisition machine
- ``fip``: FIP acquisition machine
- ``dynamicrouting``: Dynamic Routing acquisition machine
- ``smartspim``: SmartSPIM acquisition machine


Please check the ``pyproject.toml`` file for the most up-to-date list of packages and their dependencies.
If you need to install all the packages, you can do so with the following command:

.. code:: bash

    pip install aind-metadata-mapper[all]


2. Generating a Session with an ETL
------------------------------------
This repository is split up into different packages by acquisition machine. Each package defines a ``JobSettings`` object and an ``ETL`` to generate a desired model.

- ``JobSettings`` defines the information necessary to compile metadata, such as the input source, experimenter name, subject ID, and other machine-specific fields. It is defined in the ``models.py`` file in the appropriate package. Consult the ``models.py`` file for more information on the fields that can be set for your acquisition machine.
- ``ETL`` is a class that takes in the JobSettings object and generates the desired model. It is defined in the ``session.py`` file in the appropriate package. The ETL is responsible for extracting data from the input source, transforming it into the desired format, and loading it into the output directory.

To generate a session.json after the end of an experiment, you should edit the necessary fields in the models.py file.
For example, to generate a session.json for a bergamo experiment, edit the fields defined in the ``JobSettings`` in ``bergamo/models.py`` file like so:

.. code-block:: python

    from pathlib import Path
    from aind_metadata_mapper.bergamo.models import JobSettings
    from aind_metadata_mapper.bergamo.session import BergamoEtl
    # The input source is a directory containing the data to be processed. 
    # In this example, we are using a test data set from the BCI acquisition machine intended for Allen Institute use only.
    input_source = "/allen/aind/scratch/svc_aind_upload/test_data_sets/bci/061022"
    job_settings = JobSettings(
                input_source=Path(input_source),
                experimenter_full_name=["Jane Doe"],
                subject_id="655019",
                imaging_laser_wavelength=920,
                fov_imaging_depth=200,
                fov_targeted_structure="M1",
                notes=None,
            )
            bergamo_job = BergamoEtl(job_settings=job_settings)
            response = bergamo_job.run_job()

Alternatively, you could set up your script to pull information automatically from local config files or from your experiment control software. For example, if you've already saved the subject_id in a config file to start your experiment, you might wish to configure your script to scrape that information directly rather than having to re-enter it.

Once the JobSettings object is defined with the necessary information, the ETL can be run directly using ``run_job()`` to generate the desired model.

.. code-block:: python

    from aind_metadata_mapper.bergamo.session import BergamoEtl
    etl = BergamoEtl(job_settings=job_settings) 
    response = etl.run_job()
    # The response object contains the following fields:
    data = response.data # the generated session model if no output directory is specified
    message = response.message # a message describing the status of the job, e.g. validation errors
    status = response.status # the status of the job

In this example, we did not specify an output directory so our method returns a ``Session`` model. However, you can configure an output directory in the JobSettings object to have the session written to that directory as a json file.

Gather Metadata Job
--------------------
In our previous example, we used the Bergamo ETL to generate a session.json file. However, you may want to generate multiple metadata models (ex: procedures, subject, processing, etc.) at once after your experiment. 

To facilitate this, we have created a ``GatherMetadataJob`` class that allows for the generation of multiple metadata models at once. It takes in model-specific JobSettings objects and generates the desired models in parallel.

It does so with the "GatherMetadata" ``JobSettings``, defined in the ``aind_metadata_mapper.models`` package. This JobSettings object takes in a list of metadata model-specific ``JobSettings`` objects and writes the metadata to a specified output_directory. It also generates a complete meteadata.json file that contains all the models generated by the job.

We can generate a bergamo session model with the GatherMetadataJob like so:

.. code-block:: python

    from aind_metadata_mapper.bergamo.models import JobSettings as BergamoJobSettings
    from aind_metadata_mapper.bergamo.session import BergamoEtl
    from aind_metadata_mapper.models import GatherMetadataJob, SessionSettings, JobSettings as GatherMetadataJobSettings

    # 1. Define the JobSettings for a session from the desired acquisition machine
    bergamo_job_settings = BergamoJobSettings(
                input_source="/allen/aind/scratch/svc_aind_upload/test_data_sets/bci/061022",
                experimenter_full_name=["John Apple"],
                subject_id="655019",
                imaging_laser_wavelength=920,
                fov_imaging_depth=200,
                fov_targeted_structure="Primary Motor Cortex",
                notes="test upload",
    )

    # 2. Define SessionSettings object with defined job settings
    session_settings = SessionSettings(session_settings=bergamo_job_settings)
    
    # 3. Define GatherMetadataJob JobSettings with session_settings.
    # Note that you can define settings for different metadata files here
    gather_metadata_job_settings = GatherMetadataJobSettings(
        directory_to_write_to="stage",
        session_settings=session_settings,
    )

    # 4. Define GatherMetadataJob with job settings.
    gather_metadata_job = GatherMetadataJob(directory_to_write_to="stage", job_settings=gather_metadata_job_settings)

    # 5. Run the job and get the response object
    response = gather_metadata_job.run_job()

While the example above shows how to generate just a session model with the GatherMetadataJob, We can also use it to generate complete metadata!

.. code-block:: python

    from aind_metadata_mapper.bergamo.models import JobSettings as BergamoJobSettings
    from aind_metadata_mapper.bergamo.session import BergamoEtl
    from aind_metadata_mapper.models import (
        GatherMetadataJob,
        SessionSettings,
        ProceduresSettings,
        SubjectSettings,
        RawDataDescriptionSettings,
        JobSettings as GatherMetadataJobSettings
    )

    # 1. Define the JobSettings for a session from the desired acquisition machine
    bergamo_job_settings = BergamoJobSettings(
                input_source="/allen/aind/scratch/svc_aind_upload/test_data_sets/bci/061022",
                experimenter_full_name=["John Apple"],
                subject_id="655019",
                imaging_laser_wavelength=920,
                fov_imaging_depth=200,
                fov_targeted_structure="Primary Motor Cortex",
                notes="test upload",
    )

    # 2. Define SessionSettings object with defined job settings
    session_settings = SessionSettings(session_settings=bergamo_job_settings)

    # 3. Define ProceduresSettings, SubjectSettings, and RawDataDescriptionSettings
    # Note that these 3 are configured to automatically fetch metadata from databases using the aind-metadata-service
    procedures_settings = ProceduresSettings(subject_id="655019")
    subject_settings = SubjectSettings(subject_id="655019")
    raw_data_description_settings = RawDataDescriptionSettings(
        name="test",
        project_name="Ephys Platform",
        modality=Modality.ECEPHYS,
    )

    # 4. Define GatherMetadataJob JobSettings with all settings.
    gather_metadata_job_settings = GatherMetadataJobSettings(
        directory_to_write_to="stage",
        metadata_service_domain="http://aind-metadata-service",
        session_settings=session_settings,
        procedures_settings=procedures_settings,
        subject_settings=subject_settings,
        raw_data_description_settings=raw_data_description_settings,
        metadata_dir = "path/to/metadata_dir", # optional, if you want to use pre-existing metadata
        metadata_dir_force=False, # optional, if you want to force the use of pre-existing metadata
    )

    # 5. Define GatherMetadataJob with job settings.
    gather_metadata_job = GatherMetadataJob(directory_to_write_to="stage", job_settings=gather_metadata_job_settings)

    # 6. Run the job and get the response object
    response = gather_metadata_job.run_job()

Note in the example above that we are using the ``metadata_service_domain`` parameter to specify the domain of the metadata service. This is required for the GatherMetadataJob to automate procedures, subject, and raw data description generation.

Also note that the ``metadata_dir`` and ``metadata_dir_force`` parameters are optional, and are used to specify a directory containing pre-existing metadata. If these parameters are not specified, the GatherMetadataJob will generate the metadata.

- If you have pre-existing metadata, for example a rig.json file, you can specify the directory containing the file in the ``metadata_dir`` parameter. 
- If you have a pre-existing procedures.json file containing procedures not tracked in the metadata service you can specify the directory containing the procedures.json file in the ``metadata_dir`` parameter and set the ``metadata_dir_force`` parameter to True. The GatherMetadataJob will then use the procedures.json file from the specified directory.


Reporting bugs or making feature requests
-----------------------------------------
Please report any bugs or feature requests here: `issues <https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues>`_