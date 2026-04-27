"""Trading Economics US Non-Farm Payroll related labor market indicators."""

from stpstone.ingestion.countries.ww.macroeconomics.trading_econ_non_farm_payroll_related import (
	TradingEconNonFarmPayrollRelated,
)


cls_ = TradingEconNonFarmPayrollRelated(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF TRADING ECONOMICS NON FARM PAYROLL RELATED: \n{df_}")
df_.info()
