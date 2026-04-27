"""Trading Economics US wages data ingestion."""

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._trading_econ_base import _TradingEconBase


class TradingEconWagesUsa(_TradingEconBase, ABCIngestionOperations):
	"""Trading Economics US wages data ingestion class."""

	_PATH = "united-states/wages"
	_TABLE_NAME = "us_trading_economics_wages"
	_XPATH = "//table[@class='table table-hover']//td"
	_DTYPES = {
		"RELATED": str,
		"LAST": float,
		"PREVIOUS": float,
		"UNIT": str,
		"REFERENCE": str,
	}
