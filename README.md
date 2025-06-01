# stpstone <img src="img/logo_stpstone.png" align="right" width="200" style="border-radius: 15px;" alt="stpstone">

[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)
![PyPI Version](https://img.shields.io/pypi/v/stpstone)
![Linting](https://img.shields.io/badge/linting-flake8%20+%20isort-blue)
![Security](https://img.shields.io/badge/security-bandit-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![PyPI Downloads](https://img.shields.io/pypi/dm/stpstone?color=teal)
![Open Issues](https://img.shields.io/github/issues/guilhermegor/stpstone)
![Contributions Welcome](https://img.shields.io/badge/contributions-welcome-darkgreen.svg)

**stpstone** (short for *stepping stone*) is a Python framework designed for ingesting, processing, and analyzing structured and unstructured financial data. It provides tools for ETL (Extract, Transform, Load), quantitative analysis, and derivatives pricing, optimized for financial market applications.

## ✨ Key Features

### 🌍 Data Extraction

#### **🇧🇷 Brazilian Markets (B3)**

##### **Equities & Derivatives**
- [BOVESPA Cash Equities](stpstone/ingestion/countries/br/exchange/bvmf_bov.py)
- [Futures/Options Metadata](stpstone/ingestion/countries/br/exchange/options_calendar.py)
- [Consolidated Trades](stpstone/ingestion/countries/br/exchange/consolidated_trades.py)
- [After-Market Trading](stpstone/ingestion/countries/br/exchange/consolidated_trades_after_mkt.py)
- [Futures Closing Adjustments](stpstone/ingestion/countries/br/exchange/futures_closing_adj.py)
- [Historical Volatility (Sigma)](stpstone/ingestion/countries/br/exchange/historical_sigma.py)
- [BMF Interest Rates](stpstone/ingestion/countries/br/exchange/bmf_interest_rates.py)
- [Trading Hours & Sessions](stpstone/ingestion/countries/br/exchange/trading_hours.py)
- [Securities Registry](stpstone/ingestion/countries/br/exchange/securities.py)
- [Volume Analytics](stpstone/ingestion/countries/br/exchange/volumes.py)
- [Warranty Information](stpstone/ingestion/countries/br/exchange/warranty.py)

##### **Fixed Income**
- [ANBIMA 550 Bonds](stpstone/ingestion/countries/br/exchange/anbima_550_listing.py)
- [ANBIMA Indexes](stpstone/ingestion/countries/br/exchange/indexes_anbima.py)
- [ANBIMA Index Data](stpstone/ingestion/countries/br/exchange/indexes_anbima_data.py)
- [Brazilian Corporate Bonds](stpstone/ingestion/countries/br/otc/debentures.py)
- [Sovereign Debt Pricing](stpstone/analytics/pricing/sovereign_bonds/br.py)
- [ANBIMA Theoretical Portfolios](stpstone/ingestion/countries/br/exchange/indexes_theor_portf.py)
- [ANBIMA Site Information](stpstone/ingestion/countries/br/exchange/anbima_site_infos.py)

##### **Macroeconomic Data**
- [Central Bank Data (BCB Olinda)](stpstone/ingestion/countries/br/macroeconomics/olinda_bcb.py)
- [FX Rates (PTAX)](stpstone/ingestion/countries/br/macroeconomics/ptax_bcb.py)
- [Economic Time Series (SGS BCB)](stpstone/ingestion/countries/br/macroeconomics/sgs_bcb.py)
- [IBGE Statistics (SIDRA)](stpstone/ingestion/countries/br/macroeconomics/sidra_ibge.py)
- [B3 Financial Indicators](stpstone/ingestion/countries/br/macroeconomics/b3_financial_indicators.py)
- [ANBIMA PMI Forecasts](stpstone/ingestion/countries/br/macroeconomics/anbima_pmi_forecasts.py)
- [Investing.com Brazil Data](stpstone/ingestion/countries/br/macroeconomics/investingcom_br.py)
- [Yahoo Finance Brazil Rates](stpstone/ingestion/countries/br/macroeconomics/yahii_rates.py)
- [Yahoo Finance Other Data](stpstone/ingestion/countries/br/macroeconomics/yahii_others.py)

##### **Registries & Regulatory**
- [CVM Securities Data](stpstone/ingestion/countries/br/registries/cvm.py)
- [CVM Web Data](stpstone/ingestion/countries/br/registries/cvm_web.py)
- [CVM Direct Data](stpstone/ingestion/countries/br/registries/cvm_data.py)
- [B3 Trading Securities](stpstone/ingestion/countries/br/registries/b3_trd_sec.py)
- [ANBIMA Funds Data](stpstone/ingestion/countries/br/registries/anbima_data_funds.py)
- [ANBIMA Debentures Data](stpstone/ingestion/countries/br/registries/anbima_data_debentures.py)
- [Brazilian Banks Registry](stpstone/ingestion/countries/br/registries/brazillian_banks.py)
- [Mais Retorno Instruments](stpstone/ingestion/countries/br/registries/mais_retorno_instruments.py)
- [Investment Funds Bylaws](stpstone/ingestion/countries/br/bylaws/investment_funds.py)
- [Tax Records (IRS Brazil)](stpstone/ingestion/countries/br/taxation/irsbr_records.py)

#### **🇺🇸 US Markets**

##### **Equities & ETFs**
- [Tiingo EOD + Fundamentals](stpstone/ingestion/countries/us/exchange/tiingo.py)
- [Alpha Vantage (VIX/CBOE)](stpstone/ingestion/countries/us/exchange/alphavantage.py)
- [S&P 500 Constituents](stpstone/ingestion/countries/us/registries/slickcharts_indexes_components.py)
- [ETF Database](stpstone/ingestion/countries/us/registries/etfdb_vettafi.py)

##### **Macroeconomic Data**
- [FRED Economic Series](stpstone/ingestion/countries/us/macroeconomics/fred.py)
- [US Treasury Yields](stpstone/ingestion/countries/us/macroeconomics/fred.py)

#### **🌐 Worldwide Markets**

##### **Cryptocurrencies**
- [CoinMarketCap Top-500](stpstone/ingestion/countries/ww/exchange/crypto/coinmarket.py)
- [CoinCap Market Data](stpstone/ingestion/countries/ww/exchange/crypto/coincap.py)
- [CoinPaprika Analytics](stpstone/ingestion/countries/ww/exchange/crypto/coinpaprika.py)

##### **Global Markets**
- [FMP Financial Markets](stpstone/ingestion/countries/ww/exchange/markets/fmp.py)
- [ADVFN Global Data](stpstone/ingestion/countries/ww/exchange/markets/advfn.py)
- [Investing.com Global](stpstone/ingestion/countries/ww/exchange/markets/investingcom.py)
- [Yahoo Finance WebService](stpstone/ingestion/countries/ww/exchange/markets/yf_ws.py)

##### **Global Fixed Income**
- [World Government Bonds](stpstone/ingestion/countries/ww/macroeconomics/world_gov_bonds.py)
- [Global Interest Rates](stpstone/ingestion/countries/ww/macroeconomics/global_rates.py)
- [Trading Economics Data](stpstone/ingestion/countries/ww/macroeconomics/trading_economics.py)

##### **Credit Ratings & Risk**
- [S&P Global Corporate Ratings](stpstone/ingestion/countries/ww/registries/ratings_corp_spglobal.py)

----
### 🔄 Data Transformation

#### **Data Cleaning**
- [Data Cleaning Utilities](stpstone/transformations/cleaner/data_cleaning.py)
- [Exploratory Data Analysis](stpstone/transformations/cleaner/eda.py)
- [Feature Selection](stpstone/transformations/cleaner/features_selecting.py)

#### **Data Standardization**
- [DataFrame Standardization](stpstone/transformations/standardization/dataframe.py)

#### **Data Validation**
- [Type Enforcement](stpstone/transformations/validation/metaclass_type_checker.py)
- [Brazilian Docs Validation](stpstone/transformations/validation/br_docs.py)
- [DataFrame Validation](stpstone/transformations/validation/dataframe.py)
- [Bracket Balance Checker](stpstone/transformations/validation/balance_brackets.py)

----
### 📥 Data Loading

#### **Database Connectors**
- [PostgreSQL](stpstone/utils/connections/databases/postgresql.py)
- [MongoDB](stpstone/utils/connections/databases/mongodb.py)
- [Databricks](stpstone/utils/connections/databases/databricks.py)
- [MySQL](stpstone/utils/connections/databases/mysql.py)
- [SQL Server](stpstone/utils/connections/databases/sqlserver.py)
- [SQLite](stpstone/utils/connections/databases/sqlite.py)
- [Redis](stpstone/utils/connections/databases/redis.py)
- [Generic Database Interface](stpstone/utils/connections/databases/generic.py)

#### **Cloud Storage**
- [AWS S3](stpstone/utils/connections/clouds/aws_s3.py)
- [MinIO](stpstone/utils/connections/clouds/minio.py)
- [SendGrid Email](stpstone/utils/connections/clouds/sendgrid.py)

----
### 📊 Analytics

#### **Quantitative Analysis**
- [Financial Mathematics](stpstone/analytics/perf_metrics/financial_math.py)
- [Statistical Inference](stpstone/analytics/quant/statistical_inference.py)
- [Statistical Description](stpstone/analytics/quant/statistical_description.py)
- [Portfolio Allocation](stpstone/analytics/portfolio_alloc/eff.py)
- [Regression Analysis](stpstone/analytics/quant/regression.py)
- [Classification Models](stpstone/analytics/quant/classification.py)
- [Probability Distributions](stpstone/analytics/quant/prob_distributions.py)
- [Linear Algebra](stpstone/analytics/quant/linear_algebra.py)
- [Calculus Operations](stpstone/analytics/quant/calculus.py)
- [Interpolation Methods](stpstone/analytics/quant/interpolation.py)
- [Root Finding](stpstone/analytics/quant/root.py)
- [Sequences Analysis](stpstone/analytics/quant/sequences.py)
- [Statistical Charts](stpstone/analytics/quant/stats_charts.py)
- [Fit Assessment](stpstone/analytics/quant/fit_assessment.py)
- [Josephus Problem Solver](stpstone/analytics/quant/josephus_solver.py)

#### **Performance Metrics**
- [Data Deltas](stpstone/analytics/perf_metrics/data_deltas.py)
- [Earnings Quality](stpstone/analytics/perf_metrics/earnings_quality.py)
- [ROE Decomposition](stpstone/analytics/perf_metrics/roe_decomposition.py)

#### **Pricing Models**
- [Derivatives Pricing](stpstone/analytics/pricing/derivatives/)
  - [American Options](stpstone/analytics/pricing/derivatives/american_options.py)
  - [European Options](stpstone/analytics/pricing/derivatives/european_options.py)
  - [Forward Contracts](stpstone/analytics/pricing/derivatives/forward.py)
  - [Futures Contracts](stpstone/analytics/pricing/derivatives/futures.py)
- [Bond Valuation](stpstone/analytics/pricing/sovereign_bonds/br.py)
- [Debentures Pricing](stpstone/analytics/pricing/debentures.py)

#### **Risk Management**
- [Capital Risk](stpstone/analytics/risk/capital.py)
- [Liquidity Risk](stpstone/analytics/risk/liquidity.py)
- [Market Risk](stpstone/analytics/risk/market.py)
- [Yield Risk](stpstone/analytics/risk/yield_.py)

#### **Computer Arithmetic**
- [Binary Operations](stpstone/analytics/arithmetics/)
  - [Binary Comparator](stpstone/analytics/arithmetics/binary_comparator.py)
  - [Binary Converter](stpstone/analytics/arithmetics/binary_converter.py)
  - [Binary Arithmetic](stpstone/analytics/arithmetics/binary_divider.py)
  - [Bit Operations](stpstone/analytics/arithmetics/bitwise.py)
  - [Logic Gates](stpstone/analytics/arithmetics/logic_gate.py)
  - [Fraction Operations](stpstone/analytics/arithmetics/fraction.py)

----
### ⚙️ Utilities

#### **Microsoft Office Integration**
- [Excel Automation](stpstone/utils/microsoft_apps/excel.py)
- [Outlook Email Handling](stpstone/utils/microsoft_apps/outlook.py)
- [Windows OS Utilities](stpstone/utils/microsoft_apps/windows_os.py)
- [OneDrive Cloud Storage](stpstone/utils/microsoft_apps/onedrive.py)
- [Command Line Tools](stpstone/utils/microsoft_apps/cmd.py)

#### **Data Parsers**

##### **Structured Data**
- [JSON/YAML Parsing](stpstone/utils/parsers/json.py)
- [YAML Configuration](stpstone/utils/parsers/yaml.py)
- [XML/HTML Parsing](stpstone/utils/parsers/xml.py)
- [HTML Processing](stpstone/utils/parsers/html.py)
- [LXML Processing](stpstone/utils/parsers/lxml.py)
- [DataFrame Processing](stpstone/utils/parsers/pd.py)

##### **Unstructured Data**
- [PDF Text Extraction](stpstone/utils/parsers/pdf.py)
- [Image Processing](stpstone/utils/parsers/img.py)
- [Text File Handling](stpstone/utils/parsers/txt.py)
- [Color Processing](stpstone/utils/parsers/colors.py)

##### **Data Structure Parsers**
- [Array Processing](stpstone/utils/parsers/arrays.py)
- [Dictionary Operations](stpstone/utils/parsers/dicts.py)
- [List Processing](stpstone/utils/parsers/lists.py)
- [String Operations](stpstone/utils/parsers/str.py)
- [Number Processing](stpstone/utils/parsers/numbers.py)
- [Object Handling](stpstone/utils/parsers/object.py)

##### **Specialized Formats**
- [Archive Files (TGZ)](stpstone/utils/parsers/tgz.py)
- [Object Serialization](stpstone/utils/parsers/pickle.py)
- [Folder Operations](stpstone/utils/parsers/folders.py)

#### **Data Pipelines**
- [Parallel Processing](stpstone/utils/pipelines/parallel.py)
- [Asynchronous Workflows](stpstone/utils/pipelines/asynchronous.py)
- [Streaming Data Flows](stpstone/utils/pipelines/streaming.py)
- [Conditional Execution](stpstone/utils/pipelines/conditional.py)
- [Logging Integration](stpstone/utils/pipelines/logging.py)
- [Generic Pipeline](stpstone/utils/pipelines/generic.py)
- [Multiprocessing Helper](stpstone/utils/pipelines/mp_helper.py)

#### **Network Operations**
- [Network Diagnostics](stpstone/utils/connections/netops/diagnostics/network_info.py)
- [Proxy Management](stpstone/utils/connections/netops/proxies/)
  - [Free Proxy Sources](stpstone/utils/connections/netops/proxies/_free/)
  - [Proxy Load Testing](stpstone/utils/connections/netops/proxies/load_testing.py)
  - [Proxy Testing](stpstone/utils/connections/netops/proxies/test_proxy.py)
- [Web Scraping](stpstone/utils/connections/netops/scraping/)
  - [Scrape Validation](stpstone/utils/connections/netops/scraping/scrape_checker.py)
  - [User Agent Management](stpstone/utils/connections/netops/scraping/user_agents.py)

#### **Calendar & Date Utilities**
- [Brazilian Business Calendar](stpstone/utils/cals/br_bzdays.py)
- [US Business Calendar](stpstone/utils/cals/usa_bzdays.py)
- [Date Handling](stpstone/utils/cals/handling_dates.py)

#### **Geographic Data**
- [Brazilian Geography](stpstone/utils/geography/br.py)
- [Worldwide Geography](stpstone/utils/geography/ww.py)

#### **Data Providers**
- [Brazilian Providers](stpstone/utils/providers/br/)
  - [ANBIMA Data API](stpstone/utils/providers/br/abimadata_api.py)
  - [INOA Provider](stpstone/utils/providers/br/inoa.py)
  - [B3 Line API](stpstone/utils/providers/br/line_b3.py)
  - [B3 Margin Simulator](stpstone/utils/providers/br/margin_simulator_b3.py)
- [Global Providers](stpstone/utils/providers/ww/)
  - [Reuters Data](stpstone/utils/providers/ww/reuters.py)

#### **Trading & Automation**
- [MetaTrader 5](stpstone/utils/trading_platforms/mt5.py)
- [Playwright WebDriver](stpstone/utils/webdriver_tools/playwright_wd.py)
- [Selenium WebDriver](stpstone/utils/webdriver_tools/selenium_wd.py)

#### **Communication & Notifications**
- [Slack Webhooks](stpstone/utils/webhooks/slack.py)
- [Microsoft Teams](stpstone/utils/webhooks/teams.py)

#### **Other Utilities**
- [Airflow Plugins](stpstone/utils/orchestrators/airflow/plugins.py)
- [Database Logging](stpstone/utils/loggs/db_logs.py)
- [Log Creation](stpstone/utils/loggs/create_logs.py)
- [Initial Setup](stpstone/utils/loggs/init_setup.py)
- [Data Conversions](stpstone/utils/conversions/)
  - [Base Converter](stpstone/utils/conversions/base_converter.py)
  - [Expression Converter](stpstone/utils/conversions/expression_converter.py)
- [Security & Cryptography](stpstone/utils/security/secure_crypto.py)
- [System Drive Management](stpstone/utils/system/drives.py)
- [LLM Integration](stpstone/utils/llms/gpt.py)

----
### 🏗️ Data Structures & Algorithms

#### **Queue Implementations**
- [Priority Queues](stpstone/dsa/queues/priority_queues.py)
- [Simple Deque](stpstone/dsa/queues/simple_deque.py)
- [Simple Queue](stpstone/dsa/queues/simple_queue.py)

#### **Stack Implementations**
- [Simple Stack](stpstone/dsa/stacks/simple_stack.py)

#### **Tree Structures**
- [AVL Tree](stpstone/dsa/trees/avl_tree.py)
- [B-Tree](stpstone/dsa/trees/b_tree.py)


## 🚀 Getting Started

### Prerequisites:

- **Python** ^3.12

### Optional:
- **Pyenv**: [Installation Guide](https://github.com/pyenv/pyenv)
- **Makefile** (below are some benefits of usage)

    | Category	| Example Targets | Benefit |
    | --------- | --------------- | ------- |
    | Package Lifecycle	| `build_package_pypi_org`, `clean_dist` | Automated PyPI publishing |
    | Git Workflows	| `git_pull_force`, `gh_protect_main` | Enforces branch policies |
    | CI/CD	| `gh_actions_local_tests`	| Local pipeline validation |
    | Code Generation | `ingestion_concrete_creator` | Factory pattern automation |
    | Dev Environment | `vscode_setup` | Consistent IDE setup |
- Makefile Installation Options for Windows:
  - 1: [MinGW](https://sourceforge.net/projects/mingw/files/MinGW/Extension/make/mingw32-make-3.80-3/)
  - 2: [Chocolatey](https://chocolatey.org/install): Windows Package Manager
  - 2.1.: [Chocolatey Make](https://community.chocolatey.org/packages/make)
- Makefile Installation for MacOS:
  - 1: [Homebrew](https://brew.sh/): MacOS Package Manager
  - 1.1: [Homebrew Make](https://formulae.brew.sh/formula/make)
- Makefile Installation for Ubuntu: Preinstalled, no action needed

### Installation Guide:

-  **Option 1: Pip Install (Recommended)**
[📌 Available on PyPI](https://pypi.org/project/stpstone/)

    <pre style="font-size: 12px;"><code>
    #!/bin/bash
    pip install stpstone
    </code></pre>

- **Option 2: Build from Source**

    <pre style="font-size: 12px;"><code>
    #!/bin/bash
    git clone https://github.com/guilhermegor/stpstone.git
    cd stpstone
    pyenv install 3.12.8
    pyenv local 3.12.8
    poetry install --no-root
    poetry shell
    </code></pre>

- **Setting Up Make (Optional for Build Automation)**

    | Platform | Instructions |
    |----------|--------------|
    | **Windows** | Install via [MinGW](https://sourceforge.net/projects/mingw/)<br>1. Download MinGW installer<br>2. Select `mingw32-make` during installation<br>3. Add `C:\MinGW\bin` to PATH<br>4. ```mingw32-make --version```|
    | **MacOS** | Pre-installed or via Xcode CLI tools:<br>`xcode-select --install` |
    | **Linux** | Install via package manager:<br>`sudo apt-get install build-essential` |


- **Python Kernel Versioning (Pyenv)**: [pyenv instructions to installation](https://github.com/pyenv/pyenv)

## 🧪 Running Tests

Execute unit and integration tests:

<pre style="font-size: 12px;"><code>
#!/bin/bash
poetry run python -m unittest discover -s tests/unit -p "*.py" -v
poetry run python -m unittest discover -s tests/integration -p "*.py" -v
</code></pre>

## 📂 Project Structure

<pre style="font-size: 12px;"><code>
stpstone/
│
├── 📁 .github/
│   ├── 📁 workflows/         # GitHub Actions CI/CD pipelines
│   ├── 📜 CODEOWNERS         # Code ownership definitions
│   └── 📜 PULL_REQUEST_TEMPLATE.md  # PR template
│
├── 📁 .vscode/               # VSCode configuration
│   └── ⚙️ settings.json      # Editor preferences and extensions
│
├── 📁 cli/                   # Command Line Interface components
│
├── 📁 data/                  # Data storage and management
│
├── 📁 docs/                  # Project documentation
│
├── 📁 examples/              # Example implementations
│
├── 📁 img/                   # Visual assets
│
├── 📁 stpstone/              # Core Python package
│   ├── 📁 _config/           # Configuration management
│   ├── 📁 analysis/          # Analytical components
│   ├── 📁 dsa/               # Data structures & algorithms
│   ├── 📁 ingestion/         # Data ingestion pipelines
│   ├── 📁 transformations/   # Data transformation logic
│   ├── 📁 utils/             # Shared utilities
│   └── 🐍 __init__.py        # Package initialization
│
├── 📁 tests/                 # Test suites
│   ├── 📁 unit/              # Unit tests
│   ├── 📁 integration/       # Integration tests
│   └── 📁 performance/       # Performance benchmarking
│
├── 📜 .gitignore             # Git ignore patterns
├── ⚙️ .pre-commit-config.yaml # Pre-commit hook configurations
├── � .python-version        # Pyenv version specification
├── 📜 LICENSE               # MIT License file
├── ⚙️ Makefile              # Automation tasks
├── 📦 poetry.lock           # Exact dependency versions
├── ⚙️ pyproject.toml        # Project metadata and dependencies
├── 📖 README.md             # Project overview
├── 📦 requirements.txt      # Production dependencies
├── 🔧 requirements-dev.txt  # Development dependencies
└── 💻 requirements-venv.txt # Virtual environment setup
</code></pre>

## 👨‍💻 Authors

**Guilherme Rodrigues**  
[![GitHub](https://img.shields.io/badge/GitHub-guilhermegor-181717?style=flat&logo=github)](https://github.com/guilhermegor)  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Guilherme_Rodrigues-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/guilhermegor/)

## 📜 License

This project is licensed under the MIT License - see LICENSE for details.

## 🙌 Acknowledgments

* Inspired by open-source financial libraries and tools

* This documentation follows a structure inspired by [PurpleBooth's README-Template.md](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2).  

* Special thanks to Python community

## 🔗 Useful Links

* [GitHub Repository](https://github.com/guilhermegor/stpstone)

* [Issue Tracker](https://github.com/guilhermegor/stpstone/issues)