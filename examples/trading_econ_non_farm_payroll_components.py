"""Trading Economics US Non-Farm Payroll employment components breakdown."""

from stpstone.ingestion.countries.ww.macroeconomics.trading_econ_non_farm_payroll_components import (
	TradingEconNonFarmPayrollComponents,
)


cls_ = TradingEconNonFarmPayrollComponents(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF TRADING ECONOMICS NON FARM PAYROLL COMPONENTS: \n{df_}")
df_.info()
