"""Trading Economics US Non-Farm Payroll calendar forecasts."""

from stpstone.ingestion.countries.ww.macroeconomics.trading_econ_non_farm_payroll_forecasts import (
	TradingEconNonFarmPayrollForecasts,
)


cls_ = TradingEconNonFarmPayrollForecasts(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF TRADING ECONOMICS NON FARM PAYROLL FORECASTS: \n{df_}")
df_.info()
