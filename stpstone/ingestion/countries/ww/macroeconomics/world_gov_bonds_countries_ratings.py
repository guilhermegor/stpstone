"""World Government Bonds country credit ratings ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._world_gov_bonds_base import (
    _WorldGovBondsBase,
)


class WorldGovBondsCountriesRatings(_WorldGovBondsBase, ABCIngestionOperations):
    """World Government Bonds country credit ratings ingestion class."""

    _PATH = "world-credit-ratings/"
    _TABLE_NAME = "ww_wgb_countries_ratings"
    _XPATH = "//table[@class='home-rating-table sortable w3-table money pd44 -f15']//td"
    _DTYPES: dict[str, Any] = {
        "COUNTRY": str,
        "SP": "category",
        "MOODYS": "category",
        "FITCH": "category",
        "DBRS": "category",
    }
