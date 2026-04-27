"""Trading Economics US Non-Farm Payroll historical statistics ingestion."""

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._trading_econ_base import _TradingEconBase


class TradingEconNonFarmPayrollStats(_TradingEconBase, ABCIngestionOperations):
	"""Trading Economics US Non-Farm Payroll historical statistics ingestion class."""

	_PATH = "united-states/non-farm-payrolls"
	_TABLE_NAME = "us_trading_economics_non_farm_payroll_stats"
	_XPATH = "//div[@id='ctl00_ContentPlaceHolder1_ctl00_ctl03_Panel1']//table//td"
	_DTYPES = {
		"N_A": "category",
		"ACTUAL": float,
		"PREVIOUS": float,
		"HIGHEST": float,
		"LOWEST": float,
		"DATES": str,
		"UNIT": str,
		"FREQUENCY": str,
		"N_A_2": "category",
	}
