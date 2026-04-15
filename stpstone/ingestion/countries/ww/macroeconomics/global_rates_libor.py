"""Global-Rates.com LIBOR interest rates ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._global_rates_base import _GlobalRatesBase


class GlobalRatesLibor(_GlobalRatesBase, ABCIngestionOperations):
    """Global-Rates.com LIBOR interest rates ingestion class."""

    _SOURCE_NAME = "libor"
    _TABLE_NAME = "ww_globalrates_libor"
    _PATH = "en/interest-rates/libor/"
    _DTYPES: dict[str, Any] = {"DATE": "date", "RATE_NAME": str, "RATE_VALUE": float}
