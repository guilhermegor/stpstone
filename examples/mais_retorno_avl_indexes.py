"""Mais Retorno Available Indexes Paginated Listing."""

from stpstone.ingestion.countries.br.registries.mais_retorno_avl_indexes import (
	MaisRetornoAvlIndexes,
)


cls_ = MaisRetornoAvlIndexes(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF MAIS RETORNO AVAILABLE INDEXES: \n{df_}")
df_.info()
