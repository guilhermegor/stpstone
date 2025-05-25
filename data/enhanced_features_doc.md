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
- [DataFrame Contracts](stpstone/transformations/validation/dataframe.py)
- [Bracket Balance Checker](stpstone/transformations/validation/balance_brackets.py)

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