"""Global-Rates.com United States CPI inflation ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesUsaCpi(_GlobalRatesBase, ABCIngestionOperations):
	"""Global-Rates.com United States CPI inflation ingestion class."""

	_SOURCE_NAME = "usa_cpi"
	_TABLE_NAME = "ww_globalrates_usa_cpi"
	_PATH = "en/inflation/cpi/4/united-states/"
	_DTYPES: dict[str, Any] = {"DATE": str, "RATE_NAME": str, "RATE_VALUE": float}
