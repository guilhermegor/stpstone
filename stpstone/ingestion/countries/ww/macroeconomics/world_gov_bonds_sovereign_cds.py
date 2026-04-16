"""World Government Bonds sovereign CDS spreads ingestion."""

from typing import Any

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.ww.macroeconomics._world_gov_bonds_base import (
	_WorldGovBondsBase,
)


class WorldGovBondsSovereignCds(_WorldGovBondsBase, ABCIngestionOperations):
	"""World Government Bonds sovereign CDS spreads ingestion class."""

	_PATH = "sovereign-cds/"
	_TABLE_NAME = "ww_wgb_sovereign_cds"
	_XPATH = "//table[@class='w3-table sortable money pd44 -f14']//td"
	_TRIM_LAST = True
	_DTYPES: dict[str, Any] = {
		"COUNTRY": str,
		"RATING_SP": str,
		"5Y_CDS": float,
		"DELTA_1M": float,
		"DELTA_6M": float,
		"IMPLIED_PROB_DEFAULT_PD": float,
		"DATE": str,
	}
