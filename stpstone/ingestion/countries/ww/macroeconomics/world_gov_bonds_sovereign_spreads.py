"""World Government Bonds sovereign bond spreads ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._world_gov_bonds_base import (
    _WorldGovBondsBase,
)


class WorldGovBondsSovereignSpreads(_WorldGovBondsBase, ABCIngestionOperations):
    """World Government Bonds sovereign bond spreads ingestion class."""

    _PATH = ""
    _TABLE_NAME = "ww_wgb_sovereign_spreads"
    _XPATH = "//table[@class='homeBondTable sortable w3-table money pd44 -f15']//td"
    _DTYPES: dict[str, Any] = {
        "COUNTRY": str,
        "RATING_SP": str,
        "10Y_BOND_YIELD": float,
        "BANK_RATE": float,
        "SPREAD_VS_BUND": float,
        "SPREAD_VS_TNOTE": float,
        "SPREAD_VS_BANK_RATE": float,
    }
