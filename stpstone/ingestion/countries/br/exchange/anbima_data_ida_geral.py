"""Anbima Data IDA Geral historical index ingestion."""

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.exchange._anbima_data_index_base import (
	_AnbimaDataIndexBase,
)


class AnbimaDataIDAGeral(_AnbimaDataIndexBase, ABCIngestionOperations):
	"""Anbima Data IDA Geral."""

	_INDEX_FILE = "IDAGERAL-HISTORICO.xls"
	_TABLE_NAME = "br_anbima_data_indexes_ida_geral"
	_COLUMNS = [
		"INDICE",
		"DATA_REFERENCIA",
		"NUMERO_INDICE",
		"VARIACAO_DIARIA",
		"VARIACAO_MES",
		"VARIACAO_ANUAL",
		"VARIACAO_12_MESES",
		"VARIACAO_24_MESES",
		"DURATION_DU",
	]
	_DTYPES: dict[str, object] = {
		"INDICE": "category",
		"DATA_REFERENCIA": "date",
		"NUMERO_INDICE": float,
		"VARIACAO_DIARIA": float,
		"VARIACAO_MES": float,
		"VARIACAO_ANUAL": float,
		"VARIACAO_12_MESES": float,
		"VARIACAO_24_MESES": float,
		"DURATION_DU": float,
	}
