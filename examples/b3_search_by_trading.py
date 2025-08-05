# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.search_by_trading_session import SearchByTradingB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy


# session = YieldFreeProxy(
#     bool_new_proxy=True,
#     bool_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = SearchByTradingB3(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
    cls_db=None
)

# df_ = cls_.source("standardized_instruments_groups", bool_fetch=True)
# print(f"DF STANDARDIZED INSTRUMENTS GROUPS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("index_report", bool_fetch=True)
# print(f"DF INDEX REPORT B3: \n{df_}")
# df_.info()

# df_ = cls_.source("price_report", bool_fetch=True)
# print(f"DF PRICE REPORT B3: \n{df_}")
# df_.info()

# df_ = cls_.source("registration_indicator_instruments", bool_fetch=True)
# print(f"DF REGISTRATION INDICATOR INSTRUMENTS FILE B3: \n{df_}")
# df_.info()

# df_ = cls_.source("fee_daily_unit_cost", bool_fetch=True)
# print(f"DF FEE DAILY UNIT COST FILE B3: \n{df_}")
# df_.info()

# df_ = cls_.source("fee_monthly_unit_cost", bool_fetch=True)
# print(f"DF FEE MONTHLY UNIT COST FILE B3: \n{df_}")
# df_.info()

# df_ = cls_.source("primitive_risk_factors", bool_fetch=True)
# print(f"DF PRIMITIVE RISK FACTORS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("risk_formulas", bool_fetch=True)
# print(f"DF RISK FORMULAS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("variable_fees_monthly_updt", bool_fetch=True)
# print(f"DF FEE VARIABLES B3: \n{df_}")
# df_.info()

# df_ = cls_.source("daily_liquidity_limit", bool_fetch=True)
# print(f"DF DAILY LIQUIDITY LIMIT B3: \n{df_}")
# df_.info()

# df_ = cls_.source("daily_liquidity_limit_other_limits", bool_fetch=True)
# print(f"DF DAILY LIQUIDITY LIMIT - OTHER LIMITS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("tradable_security_list", bool_fetch=True)
# print(f"DF TRADABLE SECURITY LIST B3: \n{df_}")
# df_.info()

# df_ = cls_.source("tradable_security_list", bool_fetch=True)
# print(f"DF TRADABLE SECURITY LIST B3: \n{df_}")
# df_.info()

# df_ = cls_.source("mapping_otc_instrument_groups", bool_fetch=True)
# print(f"DF MAPPING OTC INSTRUMENT GROUPS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("mapping_standardized_instrument_groups", bool_fetch=True)
# print(f"DF MAPPING STANDARDIZED INSTRUMENT GROUPS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("mtm_b3", bool_fetch=True)
# print(f"DF MTM B3: \n{df_}")
# print(f"TEST HAIRCUT B3: \n{df_[df_["TICKER"] == "PETR4"]}")
# df_.info()

# df_ = cls_.source("option_theor_premiums", bool_fetch=True)
# print(f"DF OPTIONS THEORETICAL PREMIUMS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("fx_markets_traded_rates", bool_fetch=True)
# print(f"DF FX MARKETS TRADED RATES B3: \n{df_}")
# df_.info()

# df_ = cls_.source("fx_market_volume_settled", bool_fetch=True)
# print(f"DF FX MARKETS VOLUMES SETTLED B3: \n{df_}")
# df_.info()

# df_ = cls_.source("list_isin_numbers_swaps", bool_fetch=True)
# print(f"DF LIST ISIN NUMBER SWAPS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("swap_market_rates", bool_fetch=True)
# print(f"DF SWAP MARKET RATES B3: \n{df_}")
# df_.info()

# df_ = cls_.source("instrument_group_parameters", bool_fetch=True)
# print(f"DF INSTRUMENT GROUP PARAMETERS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("spot_accepted_collateral_b3", bool_fetch=True)
# print(f"DF SPOT ACCEPTED COLLATERAL B3: \n{df_}")
# df_.info()

# df_ = cls_.source("risk_scenarios_curve_types", bool_fetch=True)
# print(f"DF RISK SCENARIOS CURVE TYPES B3: \n{df_}")
# df_.info()

df_ = cls_.source("primitive_risk_factors_merged", bool_fetch=True)
print(f"DF PRIMITIVE RISK FACTORS MERGED B3: \n{df_}")
df_.info()
