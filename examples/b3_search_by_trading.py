# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.exchange.search_by_trading_session import SearchByTradingB3
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


# session = ReqSession(
#     bl_new_proxy=True,
#     bl_use_timer=True,
#     float_min_ratio_times_alive_dead=0.02,
#     float_max_timeout=600
# ).session
# print(session.proxies)

cls_ = SearchByTradingB3(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    cls_db=None
)

# df_ = cls_.source("standardized_instruments_groups", bl_fetch=True, bl_debug=False)
# print(f"DF STANDARDIZED INSTRUMENTS GROUPS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("index_report", bl_fetch=True, bl_debug=False)
# print(f"DF INDEX REPORT B3: \n{df_}")
# df_.info()

# df_ = cls_.source("price_report", bl_fetch=True, bl_debug=False)
# print(f"DF PRICE REPORT B3: \n{df_}")
# df_.info()

# df_ = cls_.source("instruments_file", bl_fetch=True, bl_debug=False)
# print(f"DF INSTRUMENTS FILE B3: \n{df_}")
# df_.info()

# df_ = cls_.source("registration_indicator_instruments", bl_fetch=True, bl_debug=False)
# print(f"DF REGISTRATION INDICATOR INSTRUMENTS FILE B3: \n{df_}")
# df_.info()

# df_ = cls_.source("fee_daily_unit_cost", bl_fetch=True, bl_debug=False)
# print(f"DF FEE DAILY UNIT COST FILE B3: \n{df_}")
# df_.info()

# df_ = cls_.source("fee_monthly_unit_cost", bl_fetch=True, bl_debug=False)
# print(f"DF FEE MONTHLY UNIT COST FILE B3: \n{df_}")
# df_.info()

# df_ = cls_.source("primitive_risk_factors", bl_fetch=True, bl_debug=False)
# print(f"DF PRIMITIVE RISK FACTORS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("risk_formulas", bl_fetch=True, bl_debug=False)
# print(f"DF RISK FORMULAS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("variable_fees_monthly_updt", bl_fetch=True, bl_debug=False)
# print(f"DF FEE VARIABLES B3: \n{df_}")
# df_.info()

# df_ = cls_.source("daily_liquidity_limit", bl_fetch=True, bl_debug=False)
# print(f"DF DAILY LIQUIDITY LIMIT B3: \n{df_}")
# df_.info()

# df_ = cls_.source("daily_liquidity_limit_other_limits", bl_fetch=True, bl_debug=False)
# print(f"DF DAILY LIQUIDITY LIMIT - OTHER LIMITS B3: \n{df_}")
# df_.info()

# df_ = cls_.source("tradable_security_list", bl_fetch=True, bl_debug=False)
# print(f"DF TRADABLE SECURITY LIST B3: \n{df_}")
# df_.info()

# df_ = cls_.source("tradable_security_list", bl_fetch=True, bl_debug=False)
# print(f"DF TRADABLE SECURITY LIST B3: \n{df_}")
# df_.info()

df_ = cls_.source("mapping_otc_instrument_groups", bl_fetch=True, bl_debug=False)
print(f"DF MAPPING OTC INSTRUMENT GROUPS B3: \n{df_}")
df_.info()
