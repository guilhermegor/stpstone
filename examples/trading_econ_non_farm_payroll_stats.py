"""Trading Economics US Non-Farm Payroll historical statistics summary."""

from stpstone.ingestion.countries.ww.macroeconomics.trading_econ_non_farm_payroll_stats import (
	TradingEconNonFarmPayrollStats,
)


cls_ = TradingEconNonFarmPayrollStats(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF TRADING ECONOMICS NON FARM PAYROLL STATS: \n{df_}")
df_.info()
