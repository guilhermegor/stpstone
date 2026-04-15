"""Global-Rates.com United Kingdom CPI inflation ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesBritishCpi(_GlobalRatesBase, ABCIngestionOperations):
    """Global-Rates.com United Kingdom CPI inflation ingestion class."""

    _SOURCE_NAME = "british_cpi"
    _TABLE_NAME = "ww_globalrates_british_cpi"
    _PATH = "en/inflation/cpi/60/united-kingdom/"
    _DTYPES: dict[str, Any] = {"DATE": str, "RATE_NAME": str, "RATE_VALUE": float}
