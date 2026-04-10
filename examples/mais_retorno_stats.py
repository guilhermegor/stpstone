"""Mais Retorno Instruments Statistics Across Time Windows."""

from stpstone.ingestion.countries.br.registries.mais_retorno_stats import (
	MaisRetornoStats,
)


cls_ = MaisRetornoStats(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF MAIS RETORNO STATS: \n{df_}")
df_.info()
