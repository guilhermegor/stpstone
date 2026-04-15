"""Global-Rates.com SONIA overnight rate ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesSonia(_GlobalRatesBase, ABCIngestionOperations):
    """Global-Rates.com SONIA overnight rate ingestion class."""

    _SOURCE_NAME = "sonia"
    _TABLE_NAME = "ww_globalrates_sonia"
    _PATH = "en/interest-rates/sonia/"
    _DTYPES: dict[str, Any] = {"DATE": "date", "RATE_NAME": str, "RATE_VALUE": float}
