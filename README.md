![alt text](image.png)

* Stylized name, shortened spelling of stepping stone;
* A Python framework for ingesting and interpreting structured and unstructured financial data, designed to optimize quantitative methods in financial markets.

## Key Features

* Data Extraction: Retrieve market data from various sources such as B3, CVM, and BACEN (Olinda);
* Quantitative Methods: Supports a range of quantitative techniques, including portfolio optimization, risk management, and financial modeling;
* Derivatives Pricing: Implements both closed-form solutions (e.g., Black-Scholes model) and open-form, iterative methods (e.g., Binomial Tree model) for pricing derivatives;
* Data Treatment: Tools for cleaning, structuring, and transforming raw financial data into usable formats for analysis;
* Data Loading: Seamlessly integrates with databases such as PostgreSQL, MySQL, and SQLite.

## Project Structure

    stpstone
    ├── airflow
    │   └── plugins.py
    ├── cals
    │   ├── br_bzdays.py
    │   ├── handling_dates.py
    │   └── usa_bzdays.py
    ├── charts
    │   ├── general_funcs.py
    │   ├── prob_and_stats.py
    │   ├── risk_management.py
    │   ├── stock_market.py
    │   └── tsir.py
    ├── cloud_clients
    │   └── aws_s3.py
    ├── document_numbers
    │   └── br.py
    ├── dsa
    │   └── trees
    │       └── b_tree.py
    ├── equity_consolidation
    │   └── banks.py
    ├── finance
    │   ├── anbima
    │   │   ├── abimadata_api.py
    │   │   ├── anbima_mtm.py
    │   │   ├── anbima_stats.py
    │   │   └── anbimadev.py
    │   ├── auditing
    │   │   └── earnings_manipulation.py
    │   ├── b3
    │   │   ├── cei.py
    │   │   ├── core.py
    │   │   ├── inoa.py
    │   │   ├── line.py
    │   │   ├── margin_simulator.py
    │   │   ├── market_data.py
    │   │   ├── search_by_trading.py
    │   │   └── up2data_web.py
    │   ├── comdinheiro
    │   │   └── api_request.py
    │   ├── cvm
    │   │   ├── cvm_data.py
    │   │   └── cvm_web.py
    │   ├── dadosdemercado_site
    │   │   └── api_request.py
    │   ├── debentures
    │   │   └── pricing.py
    │   ├── derivatives
    │   │   ├── forward.py
    │   │   ├── futures.py
    │   │   └── options
    │   │       ├── american.py
    │   │       └── european.py
    │   ├── financial_risk
    │   │   ├── capital_risk.py
    │   │   ├── liquidity_risk.py
    │   │   ├── market_risk.py
    │   │   └── yield_risk.py
    │   ├── macroeconomics
    │   │   ├── br_macro.py
    │   │   ├── global_rates.py
    │   │   ├── usa_macro.py
    │   │   └── world_gov_bonds.py
    │   ├── performance_apprraisal
    │   │   ├── company_return.py
    │   │   └── financial_math.py
    │   ├── reuters
    │   │   └── api_request.py
    │   ├── spot
    │   │   └── stocks.py
    │   └── tesouro_direto
    │       ├── calculadora.py
    │       └── consulta_dados.py
    ├── geography
    │   └── br.py
    ├── handling_data
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
    │   └── xml.py
    ├── llms
    │   └── gpt.py
    ├── loggs
    │   ├── create_logs.py
    │   └── db_logs.py
    ├── meta
    │   └── validate_pm.py
    ├── microsoft_apps
    │   ├── cmd.py
    │   ├── excel.py
    │   ├── onedrive.py
    │   ├── outlook.py
    │   └── windows_os.py
    ├── multithreading
    │   └── mp_helper.py
    ├── opening_config
    │   └── setup.py
    ├── pool_conn
    │   ├── dabricksCLI.py
    │   ├── databricks.py
    │   ├── generic.py
    │   ├── mongodb.py
    │   ├── mysql.py
    │   ├── postgresql.py
    │   ├── redis.py
    │   ├── session.py
    │   ├── sqlite.py
    │   └── sqlserver.py
    ├── quantitative_methods
    │   ├── calculus.py
    │   ├── classification.py
    │   ├── data_cleaning.py
    │   ├── eda.py
    │   ├── features_selecting.py
    │   ├── fit_assessment.py
    │   ├── interpolation.py
    │   ├── linear_algebra.py
    │   ├── prob_distributions.py
    │   ├── regression.py
    │   ├── root.py
    │   ├── sequences.py
    │   ├── statistical_description.py
    │   └── statistical_inference.py
    ├── sendgrid
    │   └── handling_sendgrid.py
    ├── settings
    │   ├── _global_slots.py
    │   ├── anbima.yaml
    │   ├── b3.yaml
    │   ├── br_macro.yaml
    │   ├── br_treasury.yaml
    │   ├── comdinheiro.yaml
    │   ├── generic.yaml
    │   ├── global_rates.yaml
    │   ├── inoa.yaml
    │   ├── llms.yaml
    │   ├── microsoft_apps.yaml
    │   ├── session.yaml
    │   ├── usa_macro.yaml
    │   └── world_gov_bonds.yaml
    ├── trading_platforms
    │   └── mt5.py
    ├── typeform_sdk_master
    │   ├── CHANGELOG.md
    │   ├── CONTRIBUTING.md
    │   ├── LICENSE
    │   ├── MANIFEST.in
    │   ├── README.md
    │   ├── requirements-dev.txt
    │   ├── setup.cfg
    │   ├── setup.py
    │   └── typeform
    │       ├── __init__.py
    │       ├── client.py
    │       ├── constants.py
    │       ├── dealing_tf.py
    │       ├── forms.py
    │       ├── responses.py
    │       ├── test
    │       │   ├── __init__.py
    │       │   ├── fixtures.py
    │       │   ├── suite.py
    │       │   ├── test_client.py
    │       │   ├── test_forms.py
    │       │   └── test_responses.py
    │       └── utils.py
    └── webhooks
        ├── slack.py
        └── teams.py




## Getting Started

These instructions will get you a copy of the project running on your local machine for development and testing purposes.

### Prerequisites

* Python ^3.9.13

### Installing

* Pyenv for Python ^3.9.13 local installation:

```powershell
(PowerShell)

Invoke-WebRequest -UseBasicParsing -Uri "https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1" -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"
```

```bash
(bash)

echo installing local version of python within project
cd "complete/path/to/project"
pyenv install 3.9.13
pyenv versions
pyenv global 3.9.13
pyenv local 3.9.13
```

* Activate poetry .venv
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