### GLOBAL CONSTANTS ###

# pypi.org libs
import os
# local libs
from stpstone.utils.parsers.yaml import reading_yaml


root_path = os.path.dirname(os.path.realpath(__file__))

# bylaws
YAML_INVESTMENT_FUNDS_BYLAWS = reading_yaml(os.path.join(root_path, 'countries/br/bylaws/investment_funds.yaml'))

# exchange
#   BR
YAML_B3_UP2DATA_REGISTRIES = reading_yaml(os.path.join(root_path, 'countries/br/exchange/up2data_registries.yaml'))
YAML_B3_UP2DATA_VOLUMES_TRD = reading_yaml(os.path.join(root_path, 'countries/br/exchange/up2data_volumes_trd.yaml'))
YAML_B3_SEARCH_BY_TRADING_SESSION = reading_yaml(os.path.join(root_path, 'countries/br/exchange/search_by_trading_session.yaml'))
YAML_B3_BVMF_BOV = reading_yaml(os.path.join(root_path, 'countries/br/exchange/bvmf_bov.yaml'))
YAML_B3_OPTIONS_CALENDAR = reading_yaml(os.path.join(root_path, 'countries/br/exchange/options_calendar.yaml'))
YAML_B3_WARRANTY = reading_yaml(os.path.join(root_path, 'countries/br/exchange/warranty.yaml'))
YAML_B3_TRADING_HOURS_B3 = reading_yaml(os.path.join(root_path, 'countries/br/exchange/trading_hours.yaml'))
#   US
YAML_US_ALPHAVANTAGE = reading_yaml(os.path.join(root_path, 'countries/us/exchange/alphavantage.yaml'))
YAML_US_TIINGO = reading_yaml(os.path.join(root_path, 'countries/us/exchange/tiingo.yaml'))
#   WW
YAML_WW_CRYPTO_COINMARKET = reading_yaml(os.path.join(root_path, 'countries/ww/exchange/crypto/coinmarket.yaml'))
YAML_WW_CRYPTO_COINPAPRIKA = reading_yaml(os.path.join(root_path, 'countries/ww/exchange/crypto/coinpaprika.yaml'))
YAML_WW_CRYPTO_COINCAP = reading_yaml(os.path.join(root_path, 'countries/ww/exchange/crypto/coincap.yaml'))
YAML_WW_FMP = reading_yaml(os.path.join(root_path, 'countries/ww/exchange/markets/fmp.yaml'))
YAML_WW_INVESTINGCOM = reading_yaml(os.path.join(root_path, 'countries/ww/exchange/markets/investingcom.yaml'))

# macroeconomics
YAML_BR_PTAX_BCB = reading_yaml(os.path.join(root_path, 'countries/br/macroeconomics/currencies/ptax_bcb.yaml'))

# otc
YAML_DEBENTURES = reading_yaml(os.path.join(root_path, 'countries/br/otc/debentures.yaml'))

# taxation
YAML_IRSBR = reading_yaml(os.path.join(root_path, 'countries/br/taxation/irsbr.yaml'))

# netops
YAML_SESSION = reading_yaml(os.path.join(root_path, 'netops/session.yaml'))


# ! deprecated
YAML_ANBIMA = reading_yaml(os.path.join(root_path, 'anbima.yaml'))
YAML_B3 = reading_yaml(os.path.join(root_path, 'b3.yaml'))
YAML_BR_TRS = reading_yaml(os.path.join(root_path, 'br_treasury.yaml'))
YAML_CD = reading_yaml(os.path.join(root_path, 'comdinheiro.yaml'))
YAML_GEN = reading_yaml(os.path.join(root_path, 'generic.yaml'))
YAML_WGBD = reading_yaml(os.path.join(root_path, 'world_gov_bonds.yaml'))
YAML_GLB_RT = reading_yaml(os.path.join(root_path, 'global_rates.yaml'))
YAML_USA_MACRO = reading_yaml(os.path.join(root_path, 'usa_macro.yaml'))
YAML_BR_MACRO = reading_yaml(os.path.join(root_path, 'br_macro.yaml'))
YAML_MICROSOFT_APPS = reading_yaml(os.path.join(root_path, 'microsoft_apps.yaml'))
YAML_LLMS = reading_yaml(os.path.join(root_path, 'llms.yaml'))