"""Global-Rates.com European HICP inflation ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesEuropeanCpi(_GlobalRatesBase, ABCIngestionOperations):
	"""Global-Rates.com European HICP inflation ingestion class."""

	_SOURCE_NAME = "european_cpi"
	_TABLE_NAME = "ww_globalrates_european_cpi"
	_PATH = "en/inflation/hicp/3/europe/"
	_DTYPES: dict[str, Any] = {"DATE": str, "RATE_NAME": str, "RATE_VALUE": float}
