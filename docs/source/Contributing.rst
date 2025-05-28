Contributor Guidelines
======================

This repository is designed to map information from different acquisition machines to aind-data-schema models, and is therefore shared amongst multiple teams.
This document will go through best practices for contributing to this project.

Issues and Feature Requests
---------------------------
Questions, feature requests and bug reports are all welcome as discussions or `issues <https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues>`_. Please use the provided `templates <https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues/new/choose>`_ to ensure we have enough information to work with.

**NOTE**: If your package requires upgrading aind-data-schema, create a separate ticket and a dedicated engineer will handle the upgrade.

Installation and Development
----------------------------
To develop the software, clone the repository and create a new branch for your changes.
Please do not fork this repository unless you are an external developer. 

.. code:: bash

    git clone git@github.com:AllenNeuralDynamics/aind-metadata-mapper.git
    git checkout -b my-new-feature-branch
 
Then in bash, run

.. code:: bash

    pip install -e .[dev]

to set up your environment. Once these steps are complete, you can get developing.

Project Organization
--------------------

This codebase is organized by acquisition machine, so if you're adding a new machine, create a new directory. Otherwise, put your code in its corresponding directory. Your package should abide to the following structure:

.. code:: bash

    my_acquisition_machine/
    |
    ├──__init__.py
    ├──{desired_model_type}.py (ex: session.py)
    ├──models.py


The ``models.py`` file should contain the ``JobSettings`` class that will be used by your ETL to produce the desired model. We've provided a ``BaseJobSettings`` class that can be used as a base class for your JobSettings. 

The ``JobSettings`` class can be used to define the fields that the user needs to input and default values for fields that will usually remain the same. It may look like this:

.. code:: python
   
      from aind_metadata_mapper.core_models import BaseJobSettings
   
      class JobSettings(BaseJobSettings):
         """Data that needs to be input by the user"
         job_settings_name: str = "my_acquisition_machine"
         # mandatory fields
         experimenter_full_name: List[str]
         subject_id: str

         # fields with default values
         some_field: str = "default_value"
         some_other_field: str = "default_value"

The {desired_model_type}.py file should contain the ``ETL`` class that will be used to extract, transform, and load the data into the desired model. 
The class should inherit from the ``GenericEtl`` class and implement the ``run_job`` method. that calls any other class methods that needed to extract, transform, and load the data into the desired model.

.. code:: python
   
   from aind_metadata_mapper.core import GenericEtl
   from aind_metadata_mapper.my_acquisition_machine.models import JobSettings
   
      class MyAcquisitionMachineETL(GenericEtl[JobSettings]):
         """Class to manage transforming data files from my_acquisition_machine into the desired model"""
         
         def __init__(self, job_settings: Union[JobSettings, str]):
            if isinstance(job_settings, str):
               job_settings_model = JobSettings.model_validate_json(job_settings)
            else:
                  job_settings_model = job_settings
            super().__init__(job_settings=job_settings_model)

         # Enter methods to extract, transform, and load the data into the desired model
         
         def run_job(self):
               # Call any other class methods that needed to extract, transform, and load the data into the desired model
               pass

Please see the bergamo package for a more complete example of how to structure your code.
Each package should also have it's own package dependencies. These should be added in the in the ``pyproject.toml`` file in the root of the repository like so:

.. code:: bash

   [project.optional-dependencies]
   my_acquisition_machine = [
       "aind-metadata-mapper[schema]",
       "some-other-package",
   ]

Unit Testing
------------

Testing is required to open a PR in this repository to ensure robustness and reliability of our codebase.

- **1:1 Correspondence**: Structure unit tests in a manner that mirrors the module structure.
  - For every package in the src directory, there should be a corresponding test package.
  - For every module in a package, there should be a corresponding unit test module.
  - For every method in a module, there should be a corresponding unit test.
- **Test Naming**: Use the following naming convention for test files and test methods:
   - Test files should be named ``test_<module_name>.py`` (e.g., ``test_session.py``).
   - Test methods should be named ``test_<method_name>_<description>`` (e.g., ``test_run_job_success``).
- **Mocking Writes**: Your unit tests should not write anything out. You can use the unittest.mock library to intercept file operations and test your method without actually creating a file.
- **Test Coverage**: Aim for comprehensive test coverage to validate all critical paths and edge cases within the module. To open a PR, you will need at least 80% coverage.
  
  - Please test your changes using the coverage library, which will run the tests and log a coverage report:
  
    .. code:: bash

        coverage run -m unittest discover && coverage report
        
  - To open the coverage report in a browser, you can run:

   .. code:: bash
      
         coverage html
 
   and find the report in the htmlcov/index.html.

Linters
-------

There are several libraries used to run linters and check documentation. We've included these in the development package. You can run them as described here.

As described above, please test your changes using the coverage library, which will run the tests and log a coverage report:

.. code:: bash

    coverage run -m unittest discover && coverage report


Use interrogate to check that modules, methods, etc. have been documented thoroughly:

.. code:: bash

    interrogate .


Use flake8 to check that code is up to standards (no unused imports, etc.):
.. code:: bash

    flake8 .

Use black to automatically format the code into PEP standards:
.. code:: bash

    black .

Use isort to automatically sort import statements:
.. code:: bash

    isort .


Integration Testing
-------------------

To ensure that an ETL runs as expected against data on the VAST, you can run an integration test locally by pointing to the input directory on VAST. For example, to test the 'bergamo' package:
.. code:: bash

    python tests/integration/bergamo/session.py --input_source "/allen/aind/scratch/svc_aind_upload/test_data_sets/bergamo" IntegrationTestBergamo


Branches and Pull requests
---------------------------
For internal members, please create a branch. For external members, please fork the repository and open a pull request from the fork. We'll primarily use Angular style for commit messages.

Branch naming conventions
~~~~~~~~~~~~~~~~~~~~~~~~~

Name your branch using the following format:
``<type>-<issue_number>-<short_summary>``

where:

-  ``<type>`` is one of:

   -  **build**: Changes that affect the build system
      or external dependencies (e.g., pyproject.toml, setup.py)
   -  **ci**: Changes to our CI configuration files and scripts
      (examples: .github/workflows/ci.yml)
   -  **docs**: Changes to our documentation
   -  **feat**: A new feature
   -  **fix**: A bug fix
   -  **perf**: A code change that improves performance
   -  **refactor**: A code change that neither fixes a bug nor adds
      a feature, but will make the codebase easier to maintain
   -  **test**: Adding missing tests or correcting existing tests
   -  **hotfix**: An urgent bug fix to our production code
-  ``<issue_number>`` references the GitHub issue this branch will close
-  ``<short_summary>`` is a brief description that shouldn’t be more than 3
   words.

Some examples:

-  ``feat-12-adds-email-field``
-  ``fix-27-corrects-endpoint``
-  ``test-43-updates-server-test``

We ask that a separate issue and branch are created if code is added
outside the scope of the reference issue.

Pull Requests
~~~~~~~~~~~~~

Pull requests and reviews are required before merging code into this
project. You may open a ``Draft`` pull request and ask for a preliminary
review on code that is currently a work-in-progress.

Before requesting a review on a finalized pull request, please verify
that the automated checks have passed first. You can review the linters section.


Release Cycles
--------------------------

For this project, we have adopted the `Git
Flow <https://www.gitkraken.com/learn/git/git-flow>`__ system. We will
strive to release new features and bug fixes on a two week cycle. The
rough workflow is:

Hotfixes
~~~~~~~~

-  A ``hotfix`` branch is created off of ``main``
-  A Pull Request into is ``main`` is opened, reviewed, and merged into
   ``main``
-  A new ``tag`` with a patch bump is created, and a new ``release`` is
   deployed
-  The ``main`` branch is merged into all other branches

Feature branches and bug fixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  A new branch is created off of ``dev``
-  A Pull Request into ``dev`` is opened, reviewed, and merged

Release branch
~~~~~~~~~~~~~~

-  A new branch ``release-v{new_tag}`` is created
-  Documentation updates and bug fixes are created off of the
   ``release-v{new_tag}`` branch.
-  Commits added to the ``release-v{new_tag}`` are also merged into
   ``dev``
-  Once ready for release, a Pull Request from ``release-v{new_tag}``
   into ``main`` is opened for final review
-  A new tag will automatically be generated
-  Once merged, a new GitHub Release is created manually

Pre-release checklist
~~~~~~~~~~~~~~~~~~~~~

-  ☐ Increment ``__version__`` in
   ``src/aind-metadata-mapper/__init__.py`` file
-  ☐ Run linters, unit tests, and integration tests
-  ☐ Verify code is deployed and tested in test environment
-  ☐ Update examples
-  ☐ Update documentation

   -  Run:

   .. code:: bash

      sphinx-apidoc -o docs/source/ src
      sphinx-build -b html docs/source/ docs/build/html

-  ☐ Update and build UML diagrams

   -  To build UML diagrams locally using a docker container:

   .. code:: bash

      docker pull plantuml/plantuml-server
      docker run -d -p 8080:8080 plantuml/plantuml-server:jetty

Post-release checklist
~~~~~~~~~~~~~~~~~~~~~~

-  ☐ Merge ``main`` into ``dev`` and feature branches
-  ☐ Edit release notes if needed
-  ☐ Post announcement


