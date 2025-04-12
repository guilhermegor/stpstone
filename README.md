![alt text](img/logo_stpstone.png)

* Stylized name, shortened spelling of stepping stone;
* A Python framework for ingesting and interpreting structured and unstructured financial data, designed to optimize quantitative methods in financial markets.

## Key Features

* Data Extraction: Retrieve market data from various sources such as B3, CVM, and BACEN (Olinda);
* Quantitative Methods: Supports a range of quantitative techniques, including portfolio optimization, risk management, and financial modeling;
* Derivatives Pricing: Implements both closed-form solutions (e.g., Black-Scholes model) and open-form, iterative methods (e.g., Binomial Tree model) for pricing derivatives;
* Data Treatment: Tools for cleaning, structuring, and transforming raw financial data into usable formats for analysis;
* Data Loading: Seamlessly integrates with databases such as PostgreSQL, MySQL, and SQLite.

## Getting Started

These instructions will get you a copy of the project running on your local machine for development and testing purposes.

### Prerequisites

* Python ^3.12

* Makefile (v3.82.90 or higher, validated with v3.82.90, but newer versions may work);

### Installing

#### Makefile:

* Windows:
    - download MinGW - [Minimalist GNU for Windows](https://sourceforge.net/projects/mingw/)
    - add C:\MinGW\bin\ (or respective installation directory) to environment variables, from the OS
    - run cli for installation and checking wheter it was succesful:
    ```bash
    (bash)
    mingw-get install mingw32-make
    mingw32-make --version
    ```
* MacOS: https://wahyu-ehs.medium.com/makefile-on-mac-os-2ef0e67b0a15
* Linux: https://stackoverflow.com/questions/3915067/what-are-makefiles-make-install-etc

#### PyPi.org

```bash
(bash)

# latest version
pip install stpstone
```

* Available at: https://pypi.org/project/stpstone/

#### Local Machine Version

* Git clone

* Pyenv for Python ^3.12.8 local installation:

```powershell
(PowerShell)

Invoke-WebRequest -UseBasicParsing -Uri "https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1" -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"
```

```bash
(bash)

echo installing local version of python within project
cd "complete/path/to/project"
pyenv install 3.12.8
pyenv versions
pyenv global 3.12.8
pyenv local 3.12.8
```

* Activate poetry .venv
```bash
(bash)

echo defining local pyenv version
pyenv global 3.12.8
pyenv which python
poetry env use "COMPLETE_PATH_PY_3.12.8"
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

## Running the Tests

* EDA - Exploratory Data Analysis:
```(bash)

(bash)

cd "complete/path/to/project"
poetry run python stpstone.tests.eda.py

```

* European / American Options:
```(bash)

(bash)

cd "complete/path/to/project"
poetry run python tests.european-american-options.py

```

* Markowitz Portfolios:
```(bash)

(bash)

cd "complete/path/to/project"
poetry run python tests.markowitz-portfolios.py

```


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
