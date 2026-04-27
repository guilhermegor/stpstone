"""Trading Economics US Non-Farm Payroll components breakdown ingestion."""

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._trading_econ_base import _TradingEconBase


class TradingEconNonFarmPayrollComponents(_TradingEconBase, ABCIngestionOperations):
	"""Trading Economics US Non-Farm Payroll components breakdown ingestion class."""

	_PATH = "united-states/non-farm-payrolls"
	_TABLE_NAME = "us_trading_economics_non_farm_payroll_components"
	_XPATH = (
		"//div[@id='ctl00_ContentPlaceHolder1_ctl00_ctl02_PanelComponents']"
		"//table[@class='table table-hover']//td"
	)
	_DTYPES = {
		"COMPONENTS": str,
		"LAST": float,
		"PREVIOUS": float,
		"UNIT": str,
		"REFERENCE": str,
	}
