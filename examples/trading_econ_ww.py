from stpstone.ingestion.countries.ww.macroeconomics.trading_economics import TradingEconWW


cls_ = TradingEconWW()

# df_ = cls_.source("non_farm_payroll_forecasts", bl_fetch=True)
# print(f"DF NON FARM PAYROLL FORECASTS: \n{df_}")
# df_.info()

# df_ = cls_.source("non_farm_payroll_components", bl_fetch=True)
# print(f"DF NON FARM PAYROLL COMPONENTS: \n{df_}")
# df_.info()

# df_ = cls_.source("non_farm_payroll_related", bl_fetch=True)
# print(f"DF NON FARM PAYROLL RELATED: \n{df_}")
# df_.info()

# df_ = cls_.source("non_farm_payroll_stats", bl_fetch=True)
# print(f"DF NON FARM PAYROLL STATS: \n{df_}")
# df_.info()

df_ = cls_.source("wages_usa", bl_fetch=True)
print(f"DF WAGES USA: \n{df_}")
df_.info()