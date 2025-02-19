![alt text](img/logo_stpstone.png)

* Stylized name, shortened spelling of stepping stone;
* A Python framework for ingesting and interpreting structured and unstructured financial data, designed to optimize quantitative methods in financial markets.

## Key Features

* Data Extraction: Retrieve market data from various sources such as B3, CVM, and BACEN (Olinda);
* Quantitative Methods: Supports a range of quantitative techniques, including portfolio optimization, risk management, and financial modeling;
* Derivatives Pricing: Implements both closed-form solutions (e.g., Black-Scholes model) and open-form, iterative methods (e.g., Binomial Tree model) for pricing derivatives;
* Data Treatment: Tools for cleaning, structuring, and transforming raw financial data into usable formats for analysis;
* Data Loading: Seamlessly integrates with databases such as PostgreSQL, MySQL, and SQLite.

## Project Structure
```
stpstone
├── __init__.py
├── _config
│   ├── anbima.yaml
│   ├── b3.yaml
│   ├── br
│   │   ├── bylaws
│   │   │   └── investment_funds.yaml
│   │   ├── exchange
│   │   │   ├── search_by_trading_session.yaml
│   │   │   ├── up2data_registries.yaml
│   │   │   ├── up2data_volumes_trd.yaml
│   │   │   └── warranty.yaml
│   │   ├── otc
│   │   │   └── debentures.yaml
│   │   └── taxation
│   │       └── irsbr.yaml
│   ├── br_macro.yaml
│   ├── br_treasury.yaml
│   ├── comdinheiro.yaml
│   ├── generic.yaml
│   ├── global_rates.yaml
│   ├── global_slots.py
│   ├── inoa.yaml
│   ├── llms.yaml
│   ├── microsoft_apps.yaml
│   ├── netops
│   │   └── session.yaml
│   ├── usa_macro.yaml
│   ├── world_gov_bonds.yaml
│   └── ww
│       └── exchange
│           └── crypto
│               ├── coincap.yaml
│               ├── coinmarket.yaml
│               └── coinpaprika.yaml
├── analytics
│   ├── anbima
│   │   ├── abimadata_api.py
│   │   ├── anbima_mtm.py
│   │   └── anbima_stats.py
│   ├── auditing
│   │   └── earnings_manipulation.py
│   ├── b3
│   │   ├── market_data.py
│   │   └── search_by_trading.py
│   ├── comdinheiro
│   │   └── api_request.py
│   ├── cvm
│   │   ├── cvm_data.py
│   │   └── cvm_web.py
│   ├── llms
│   │   └── gpt.py
│   ├── macroeconomics
│   │   ├── br_macro.py
│   │   ├── global_rates.py
│   │   ├── usa_macro.py
│   │   └── world_gov_bonds.py
│   ├── performance_apprraisal
│   │   ├── company_return.py
│   │   └── financial_math.py
│   ├── pricing
│   │   ├── debentures
│   │   │   └── pricing.py
│   │   ├── derivatives
│   │   │   ├── forward.py
│   │   │   ├── futures.py
│   │   │   └── options
│   │   │       ├── american.py
│   │   │       └── european.py
│   │   └── tesouro_direto
│   │       ├── calculadora.py
│   │       └── consulta_dados.py
│   ├── quant
│   │   ├── calculus.py
│   │   ├── classification.py
│   │   ├── fit_assessment.py
│   │   ├── interpolation.py
│   │   ├── linear_algebra.py
│   │   ├── prob_distributions.py
│   │   ├── regression.py
│   │   ├── root.py
│   │   ├── sequences.py
│   │   ├── statistical_description.py
│   │   ├── statistical_inference.py
│   │   └── stats_charts.py
│   ├── reuters
│   │   └── api_request.py
│   ├── risk_assessment
│   │   ├── capital.py
│   │   ├── liquidity.py
│   │   ├── market.py
│   │   └── yield.py
│   └── spot
│       └── stocks.py
├── dsa
│   └── trees
│       └── b_tree.py
├── ingestion
│   ├── abc
│   │   └── requests.py
│   ├── mtm
│   │   └── br
│   │       └── otc
│   │           └── debentures.py
│   ├── registries
│   │   └── br
│   │       ├── bylaws
│   │       │   └── investment_funds.py
│   │       ├── exchange
│   │       │   ├── securities.py
│   │       │   └── warranty.py
│   │       └── taxation
│   │           └── irsbr_records.py
│   └── tradings
│       ├── br
│       │   └── exchange
│       │       ├── search_by_trading_session.py
│       │       └── volumes.py
│       └── ww
│           └── exchange
│               └── crypto
│                   ├── coincap.py
│                   ├── coinmarket.py
│                   └── coinpaprika.py
├── transformations
│   ├── cleaner
│   │   ├── data_cleaning.py
│   │   ├── eda.py
│   │   └── features_selecting.py
│   ├── standardization
│   │   └── dataframe.py
│   └── validation
│       ├── br_docs.py
│       ├── dataframe.py
│       └── metaclass_type_checker.py
└── utils
    ├── cals
    │   ├── br_bzdays.py
    │   ├── handling_dates.py
    │   └── usa_bzdays.py
    ├── connections
    │   ├── clouds
    │   │   ├── aws_s3.py
    │   │   └── sendgrid.py
    │   ├── databases
    │   │   ├── dabricksCLI.py
    │   │   ├── databricks.py
    │   │   ├── generic.py
    │   │   ├── mongodb.py
    │   │   ├── mysql.py
    │   │   ├── postgresql.py
    │   │   ├── redis.py
    │   │   ├── sqlite.py
    │   │   └── sqlserver.py
    │   └── netops
    │       ├── manager.py
    │       └── session.py
    ├── exchange
    │   ├── br
    │   │   ├── inoa.py
    │   │   ├── line.py
    │   │   └── margin_simulator_b3.py
    │   └── ww
    │       └── yfinance.py
    ├── geography
    │   └── br.py
    ├── loggs
    │   ├── create_logs.py
    │   ├── db_logs.py
    │   └── init_setup.py
    ├── microsoft_apps
    │   ├── cmd.py
    │   ├── excel.py
    │   ├── onedrive.py
    │   ├── outlook.py
    │   └── windows_os.py
    ├── orchestrators
    │   └── airflow
    │       └── plugins.py
    ├── parsers
    │   ├── dicts.py
    │   ├── folders.py
    │   ├── html.py
    │   ├── img.py
    │   ├── json.py
    │   ├── lists.py
    │   ├── lxml.py
    │   ├── numbers.py
    │   ├── object.py
    │   ├── pd.py
    │   ├── pdf.py
    │   ├── pickle.py
    │   ├── str.py
    │   ├── tgz.py
    │   ├── txt.py
    │   ├── xml.py
    │   └── yaml.py
    ├── pipelines
    │   ├── asynchronous.py
    │   ├── conditional.py
    │   ├── generic.py
    │   ├── logging.py
    │   ├── mp_helper.py
    │   ├── parallel.py
    │   └── streaming.py
    ├── trading_platforms
    │   └── mt5.py
    └── webhooks
        ├── slack.py
        └── teams.py
```


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
