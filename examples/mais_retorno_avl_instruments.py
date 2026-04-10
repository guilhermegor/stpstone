"""Mais Retorno Available Instruments Paginated Listing."""

from stpstone.ingestion.countries.br.registries.mais_retorno_avl_instruments import (
	MaisRetornoAvlInstruments,
)


cls_ = MaisRetornoAvlInstruments(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF MAIS RETORNO AVAILABLE INSTRUMENTS: \n{df_}")
df_.info()
