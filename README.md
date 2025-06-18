# aind-metadata-mapper

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-86%25-yellow?logo=codecov)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

Repository to contain code that will parse source files into aind-data-schema models.

## Usage

## Installation
To use the software, in the root directory, run
```bash
pip install -e ".[all]"
```

It's possible to install just a small subset of dependencies. For example,
```bash
pip install -e ".[bergamo]"
```

To develop the code, run
```bash
pip install -e ".[dev]"
```

## Issues and Discussions
If you've found a bug in the schemas or would like to make a minor change, open an [issue](https://github.com/AllenNeuralDynamics/aind-metadata-mapper/issues) and please use the provided templates. If you'd like to propose a large change or addition, or generally have a question about how things work, head start a new Discussion!

## Contributing
Contributions are more than welcome for this project! If you'd like to develop the code, please follow the standards outlined in the [contribution guide](https://github.com/AllenNeuralDynamics/aind-metadata-mapper/blob/dev/CONTRIBUTING.md).

### Documentation
To generate the rst files source files for documentation, run
```bash
sphinx-apidoc -o docs/source/ src 
```
Then to create the documentation HTML files, run
```bash
sphinx-build -b html docs/source/ docs/build/html
```
More info on sphinx installation can be found [here](https://www.sphinx-doc.org/en/master/usage/installation.html).


More information including a user guide and contributor guidelines can be found at [readthedocs](https://aind-metadata-mapper.readthedocs.io/en/latest/).