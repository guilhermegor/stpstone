"""Global-Rates.com ESTER overnight rate ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesEster(_GlobalRatesBase, ABCIngestionOperations):
	"""Global-Rates.com ESTER overnight rate ingestion class."""

	_SOURCE_NAME = "ester"
	_TABLE_NAME = "ww_globalrates_ester"
	_PATH = "en/interest-rates/ester/"
	_DTYPES: dict[str, Any] = {"DATE": "date", "RATE_NAME": str, "RATE_VALUE": float}
