"""Global-Rates.com EURIBOR interest rates ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesEuribor(_GlobalRatesBase, ABCIngestionOperations):
    """Global-Rates.com EURIBOR interest rates ingestion class."""

    _SOURCE_NAME = "euribor"
    _TABLE_NAME = "ww_globalrates_euribor"
    _PATH = "en/interest-rates/euribor/"
    _DTYPES: dict[str, Any] = {"DATE": "date", "RATE_NAME": str, "RATE_VALUE": float}
