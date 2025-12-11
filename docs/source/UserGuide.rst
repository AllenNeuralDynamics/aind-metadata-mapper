User Guide
==========
Thank you for using ``aind-metadata-mapper``! This guide is intended for scientists and engineers in AIND that wish to generate metadata models (particularly the session model) from specific acquisition machines.
This repository is designed to centralize code to ``Transform``, and ``Load`` data from multiple acquisition machines.

1. Installation
----------------
To install with pip:

.. code:: bash

    pip install aind-metadata-mapper

2. Usage
------------------------------------
To validate and write processing metadata to a json file in the current working directory via the command:

.. code:: bash

    python -m aind_metadata_mapper.gather_processing_job --job-settings '{"output_directory": ".", "processing": {"object_type": "Processing", "describedBy": "https://raw.githubusercontent.com/AllenNeuralDynamics/aind-data-schema/main/src/aind_data_schema/core/processing.py", "schema_version": "2.1.1", "data_processes": [{"object_type": "Data process", "process_type": "Compression", "name": "Compression", "stage": "Processing", "code": {"object_type": "Code", "url": "www.example.com/ephys_compression", "parameters": {"compression_name": "BLOSC"}}, "experimenters": ["AIND Scientific Computing"], "start_date_time": "2024-10-10T01:02:03-07:00", "end_date_time": "2024-10-11T01:02:03-07:00", "output_parameters": {}}, {"object_type": "Data process", "process_type": "Other", "name": "Other", "stage": "Processing", "code": {"object_type": "Code", "url": ""}, "experimenters": ["AIND Scientific Computing"], "start_date_time": "2024-10-10T01:02:03-07:00", "end_date_time": "2024-10-11T01:02:04-07:00", "output_parameters": {}, "notes": "Data was copied."}]}}'

Reporting bugs or making feature requests
-----------------------------------------
Please report any bugs or feature requests here: `issues <https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues>`_
