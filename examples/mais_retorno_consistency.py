"""Mais Retorno Instruments Consistency Metrics."""

from stpstone.ingestion.countries.br.registries.mais_retorno_consistency import (
	MaisRetornoConsistency,
)


cls_ = MaisRetornoConsistency(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF MAIS RETORNO CONSISTENCY: \n{df_}")
df_.info()
