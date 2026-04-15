"""Global-Rates.com Canada CPI inflation ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesCanadianCpi(_GlobalRatesBase, ABCIngestionOperations):
    """Global-Rates.com Canada CPI inflation ingestion class."""

    _SOURCE_NAME = "canadian_cpi"
    _TABLE_NAME = "ww_globalrates_canadian_cpi"
    _PATH = "en/inflation/cpi/37/canada/"
    _DTYPES: dict[str, Any] = {"DATE": str, "RATE_NAME": str, "RATE_VALUE": float}
