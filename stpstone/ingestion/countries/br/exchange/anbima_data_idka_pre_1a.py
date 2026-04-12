"""Anbima Data IDKA Pre 1 Ano historical index ingestion."""

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.exchange._anbima_data_index_base import (
	_AnbimaDataIndexBase,
)


class AnbimaDataIDKAPre1A(_AnbimaDataIndexBase, ABCIngestionOperations):
	"""Anbima Data IDKA Pré 1 Ano Geral."""

	_INDEX_FILE = "IDKAPRE1A-HISTORICO.xls"
	_TABLE_NAME = "br_anbima_data_indexes_idka_pre_1a"
	_COLUMNS = [
		"INDICE",
		"DATA_REFERENCIA",
		"NUMERO_INDICE",
		"VARIACAO_DIARIA",
		"VARIACAO_MES",
		"VARIACAO_ANUAL",
		"VARIACAO_12_MESES",
	]
	_DTYPES: dict[str, object] = {
		"INDICE": "category",
		"DATA_REFERENCIA": "date",
		"NUMERO_INDICE": float,
		"VARIACAO_DIARIA": float,
		"VARIACAO_MES": float,
		"VARIACAO_ANUAL": float,
		"VARIACAO_12_MESES": float,
	}
