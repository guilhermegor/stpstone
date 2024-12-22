# PY Automation Framework

* Raw framework for automation, preparing dataset and extracting value from information
* Build with MVC design pattern (Model-View-Controller)
* Model: encapsulates data and logic
* View: handles the presentation and user interface
* Controller: manages user input and updates the Model and View accordingly

![alt text](pics/mvc_arch.png)

[freeCodeCamp - The MVC Architecture](https://www.freecodecamp.org/news/the-model-view-controller-pattern-mvc-architecture-and-frameworks-explained/)


## Getting Started

Instructions in order to run a local copy of the project, for development, deployment or running tests.

### Prerequisites

* Python ^3.9.13

### Installing

* pyenv for python ^3.9.13 local installation:

```powershell
(PowerShell)

Invoke-WebRequest -UseBasicParsing -Uri "https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1" -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"
```

```bash
(bash)

echo installing local version of python within project
cd "complete\path\project"
pyenv install 3.9.13
pyenv versions
pyenv global 3.9.13
pyenv local 3.9.13
```

* activate poetry .venv
```bash
(bash)

echo defining local pyenv version
pyenv local 3.9.13
pyenv which python
poetry env use "COMPLETE_PATH_PY_3.9.13"
echo check python version running locally
poetry run py --version

echo installing poetry .venv
poetry init
poery install --no-root

echo running current .venv
poetry shell
poetry add <package name, optionally version>
poetry run <module.py>
```

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment


## Built With


## Versioning


## Authors

**Guilherme Rodrigues** 
* [GitHub](https://github.com/guilhermegor)
* [LinkedIn](https://www.linkedin.com/in/guilhermegor/)

## License


## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

## Inspirations

* [Gist](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)