"""Global-Rates.com SOFR overnight rate ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesSofr(_GlobalRatesBase, ABCIngestionOperations):
    """Global-Rates.com SOFR overnight rate ingestion class."""

    _SOURCE_NAME = "sofr"
    _TABLE_NAME = "ww_globalrates_sofr"
    _PATH = "en/interest-rates/sofr/"
    _DTYPES: dict[str, Any] = {"DATE": "date", "RATE_NAME": str, "RATE_VALUE": float}
