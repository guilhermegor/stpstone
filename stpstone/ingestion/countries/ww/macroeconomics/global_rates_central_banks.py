"""Global-Rates.com central bank policy rates ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesCentralBanks(_GlobalRatesBase, ABCIngestionOperations):
    """Global-Rates.com central bank policy rates ingestion class."""

    _SOURCE_NAME = "central_banks"
    _TABLE_NAME = "ww_globalrates_central_banks"
    _PATH = "en/interest-rates/central-banks/"
    _DTYPES: dict[str, Any] = {
        "CENTRAL_BANK": str,
        "COUNTRY_REGION": str,
        "CURRENT": float,
        "DIRECTION": str,
        "PREVIOUS": float,
        "CHANGE": "date",
    }
