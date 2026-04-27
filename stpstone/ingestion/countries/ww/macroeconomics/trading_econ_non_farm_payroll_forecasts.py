"""Trading Economics US Non-Farm Payroll forecasts ingestion."""

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._trading_econ_base import _TradingEconBase


class TradingEconNonFarmPayrollForecasts(_TradingEconBase, ABCIngestionOperations):
	"""Trading Economics US Non-Farm Payroll forecasts ingestion class."""

	_PATH = "united-states/non-farm-payrolls"
	_TABLE_NAME = "us_trading_economics_non_farm_payroll_forecasts"
	_XPATH = "//table[@id='calendar']/tbody/tr/td"
	_DTYPES = {
		"CALENDAR": "date",
		"GMT": "category",
		"N_A": "category",
		"REFERENCE": str,
		"ACTUAL": str,
		"PREVIOUS": str,
		"CONSENSUS": str,
		"TE_FORECAST": str,
	}
