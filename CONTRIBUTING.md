# Contributor Guidelines
This repository is designed to map information from different acquisition machines to aind-data-schema models, and is therefore shared amongst multiple teams.  

This document will go through best practices for contributing to this project.

## Issues and Feature Requests
Questions, feature requests and bug reports are all welcome as [discussions or issues](https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues). Please use the provided [templates](https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues/new/choose) to ensure we have enough information to work with.

**NOTE:** If your package requires upgrading **aind-data-schema**, create a separate ticket and a dedicated engineer will handle the upgrade.

## Installation and Development
To develop the software, *clone* the repository and create a new branch for your changes.
Please do not fork this repository unless you are an external developer. 

```bash
git clone git@github.com:AllenNeuralDynamics/aind-metadata-mapper.git
git checkout -b my-new-feature-branch
``` 
Then in bash, run
```bash
pip install -e .[dev]
```
to set up your environment. Once these steps are complete, you can get developing🚀. 


### Project Organization
This codebase is organized by acquisition machine, so if you're adding a new machine, create a new directory. Otherwise, put your code in its corresponding directory. Your package should abide to the following structure:

```bash
acquisition_machine/
|
├──__init__.py
├──{desired_model_type}.py (ex: session.py)
├──models.py
```

Please define a JobSettings class in the models module which will be used by your ETL to produce the desired model. The JobSettings in the bergamo dir can be used as reference.  

### Unit Testing

Testing is required to open a PR in this repository to ensure robustness and reliability of our codebase. 
- **1:1 Correspondence:** Structure unit tests in a manner that mirrors the module structure. 
  - For every package in the src directory, there should be a corresponding test package.
  - For every module in a package, there should be a corresponding unit test module.
  - For every method in a module, there should be a corresponding unit test.
- **Mocking Writes**: Your unit tests should not write anything out. You can use the `unittest.mock` library to intercept file operations and test your method without actually creating a file.
- **Test Coverage:** Aim for comprehensive test coverage to validate all critical paths and edge cases within the module. To open a PR, you will need at least 80% coverage. 
  - Please test your changes using the **coverage** library, which will run the tests and log a coverage report:
    ```bash
    coverage run -m unittest discover && coverage report
    ```
    To open the coverage report in a browser, you can run
    ```bash
    coverage html
    ```
    and find the report in the htmlcov/index.html.
  
There are several libraries used to run linters and check documentation. We've included these in the development package. You can run them as described [here](https://github.com/AllenNeuralDynamics/aind-metadata-mapper/blob/main/README.md#linters-and-testing). 

### Integration Testing
To ensure that an ETL runs as expected against data on the VAST, you can run an integration test locally by pointing to the input directory on VAST. For example, to test the 'bergamo' package:
```bash
    python tests/integration/bergamo/session.py --input_source "/allen/aind/scratch/svc_aind_upload/test_data_sets/bergamo" IntegrationTestBergamo
 ```
 

### Linters
There are several libraries used to run linters, check documentation, and run tests.

- Please test your changes using the **coverage** library, which will run the tests and log a coverage report:

```bash
coverage run -m unittest discover && coverage report
```

- Use **interrogate** to check that modules, methods, etc. have been documented thoroughly:

```bash
interrogate .
```

- Use **flake8** to check that code is up to standards (no unused imports, etc.):
```bash
flake8 .
```

- Use **black** to automatically format the code into PEP standards:
```bash
black .
```

- Use **isort** to automatically sort import statements:
```bash
isort .
```


### Pull requests

For internal members, please create a branch. For external members, please fork the repository and open a pull request from the fork. We'll primarily use [Angular](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#commit) style for commit messages. Roughly, they should follow the pattern:
```text
<type>(<scope>): <short summary>
```

where scope (optional) describes the packages affected by the code changes and type (mandatory) is one of:

- **build**: Changes that affect build tools or external dependencies (example scopes: pyproject.toml, setup.py)
- **ci**: Changes to our CI configuration files and scripts (examples: .github/workflows/ci.yml)
- **docs**: Documentation only changes
- **feat**: A new feature
- **fix**: A bugfix
- **perf**: A code change that improves performance
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **test**: Adding missing tests or correcting existing tests

### Semantic Release

The table below, from [semantic release](https://github.com/semantic-release/semantic-release), shows which commit message gets you which release type when `semantic-release` runs (using the default configuration):

| Commit message                                                                                                                                                                                   | Release type                                                                                                    |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `fix(pencil): stop graphite breaking when too much pressure applied`                                                                                                                             | ~~Patch~~ Fix Release, Default release                                                                          |
| `feat(pencil): add 'graphiteWidth' option`                                                                                                                                                       | ~~Minor~~ Feature Release                                                                                       |
| `perf(pencil): remove graphiteWidth option`<br><br>`BREAKING CHANGE: The graphiteWidth option has been removed.`<br>`The default graphite width of 10mm is always used for performance reasons.` | ~~Major~~ Breaking Release <br /> (Note that the `BREAKING CHANGE: ` token must be in the footer of the commit) |