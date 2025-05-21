import os
from stpstone.utils.parsers.yaml import reading_yaml


root_path = os.path.dirname(os.path.realpath(__file__))


# * generic
YAML_GEN = reading_yaml(os.path.join(root_path, "generic.yaml"))

# * bylaws
YAML_INVESTMENT_FUNDS_BYLAWS = reading_yaml(
    os.path.join(root_path, "countries/br/bylaws/investment_funds.yaml")
)

# * exchange
#   BR
YAML_B3_UP2DATA_REGISTRIES = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/up2data_registries.yaml"))
YAML_B3_UP2DATA_VOLUMES_TRD = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/up2data_volumes_trd.yaml"))
YAML_B3_SEARCH_BY_TRADING_SESSION = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/search_by_trading_session.yaml"))
YAML_B3_BVMF_BOV = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/bvmf_bov.yaml"))
YAML_B3_OPTIONS_CALENDAR = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/options_calendar.yaml"))
YAML_B3_WARRANTY = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/warranty.yaml"))
YAML_B3_TRADING_HOURS_B3 = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/trading_hours.yaml"))
YAML_B3_INDEXES_THEOR_PORTF = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/indexes_theor_portf.yaml"))
YAML_B3_HISTORICAL_SIGMA = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/historical_sigma.yaml"))
YAML_B3_CONSOLIDATED_TRDS = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/consolidated_trades.yaml"))
YAML_B3_CONSOLIDATED_TRDS_AFTER_MKT = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/consolidated_trades_after_mkt.yaml"))
YAML_B3_FUTURES_CLOSING_ADJ = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/futures_closing_adj.yaml"))
YAML_ANBIMA_DATA_INDEXES = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/indexes_anbima_data.yaml"))
YAML_ANBIMA_INDEXES = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/indexes_anbima.yaml"))
YAML_ANBIMA_550_LISTING = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/anbima_550_listing.yaml"))
YAML_BMF_INTEREST_RATES = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/bmf_interest_rates.yaml"))
YAML_ANBIMA_INFOS = reading_yaml(
    os.path.join(root_path, "countries/br/exchange/anbima_site_infos.yaml"))
#   US
YAML_US_ALPHAVANTAGE = reading_yaml(
    os.path.join(root_path, "countries/us/exchange/alphavantage.yaml"))
YAML_US_TIINGO = reading_yaml(
    os.path.join(root_path, "countries/us/exchange/tiingo.yaml"))
#   WW
YAML_WW_CRYPTO_COINMARKET = reading_yaml(
    os.path.join(root_path, "countries/ww/exchange/crypto/coinmarket.yaml"))
YAML_WW_CRYPTO_COINPAPRIKA = reading_yaml(
    os.path.join(root_path, "countries/ww/exchange/crypto/coinpaprika.yaml"))
YAML_WW_CRYPTO_COINCAP = reading_yaml(
    os.path.join(root_path, "countries/ww/exchange/crypto/coincap.yaml"))
YAML_WW_FMP = reading_yaml(
    os.path.join(root_path, "countries/ww/exchange/markets/fmp.yaml"))
YAML_WW_INVESTINGCOM = reading_yaml(
    os.path.join(root_path, "countries/ww/exchange/markets/investingcom.yaml"))

# * macroeconomics
# BR
YAML_BR_PTAX_BCB = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/ptax_bcb.yaml"))
YAML_ANBIMA_FORECASTS = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/anbima_pmi_forecasts.yaml"))
YAML_YAHII_RATES = reading_yaml(os.path.join(root_path, "countries/br/macroeconomics/yahii_rates.yaml"))
YAML_YAHII_OHTERS = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/yahii_others.yaml"))
YAML_SIDRA_IBGE = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/sidra_ibge.yaml"))
YAML_SGS_BCB = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/sgs_bcb.yaml"))
YAML_B3_FINANCIAL_INDICATORS = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/b3_financial_indicators.yaml"))
YAML_OLINDA_BCB = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/olinda_bcb.yaml"))
YAML_INVESTINGCOM_BR = reading_yaml(
    os.path.join(root_path, "countries/br/macroeconomics/investingcom_br.yaml"))
# US
YAML_FRED_US = reading_yaml(
    os.path.join(root_path, "countries/us/macroeconomics/fred.yaml"))
# WW
YAML_WW_GR = reading_yaml(
    os.path.join(root_path, "countries/ww/macroeconomics/global_rates.yaml"))
YAML_WW_TRADING_ECON = reading_yaml(
    os.path.join(root_path, "countries/ww/macroeconomics/trading_economics.yaml"))
YAML_WW_WORLD_GOV_BONDS = reading_yaml(
    os.path.join(root_path, "countries/ww/macroeconomics/world_gov_bonds.yaml"))
YAML_WW_RATINGS_AGENCIES = reading_yaml(
    os.path.join(root_path, "countries/ww/macroeconomics/ratings_agencies.yaml"))

# * otc
# BR
YAML_DEBENTURES = reading_yaml(
    os.path.join(root_path, "countries/br/otc/debentures.yaml"))

# * registries
# BR
YAML_BR_CVM_REGISTRIES = reading_yaml(
    os.path.join(root_path, "countries/br/registries/cvm.yaml"))
YAML_BR_BCB_BRAZILLIAN_BANKS_REGISTRY = reading_yaml(
    os.path.join(root_path, "countries/br/registries/brazillian_banks.yaml"))
YAML_B3_TRD_SEC = reading_yaml(
    os.path.join(root_path, "countries/br/registries/b3_trd_sec.yaml"))
YAML_ANBIMA_DATA_FUNDS = reading_yaml(
    os.path.join(root_path, "countries/br/registries/anbima_data_funds.yaml"))
YAML_ANBIMA_DATA_DEBENTURES = reading_yaml(
    os.path.join(root_path, "countries/br/registries/anbima_data_debentures.yaml"))
YAML_ANBIMA_DATA_API = reading_yaml(
    os.path.join(root_path, "countries/br/registries/anbima_data_api.yaml"))
YAML_MAIS_RETORNO_FUNDS = reading_yaml(
    os.path.join(root_path, "countries/br/registries/mais_retorno_instruments.yaml"))
# US
YAML_US_SLICKCHARTS_INDEXES_COMPONENTS = reading_yaml(
    os.path.join(root_path, "countries/us/registries/slickcharts_indexes_components.yaml"))
YAML_US_ETFDB_VETTAFI = reading_yaml(
    os.path.join(root_path, "countries/us/registries/etfdb_vettafi.yaml"))
# WW
YAML_WW_RATINGS_CORP_S_AND_P = reading_yaml(
    os.path.join(root_path, "countries/ww/registries/ratings_corp_spglobal.yaml"))

# * taxation
# BR
YAML_IRSBR = reading_yaml(os.path.join(root_path, "countries/br/taxation/irsbr.yaml"))
