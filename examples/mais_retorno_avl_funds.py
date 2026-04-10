"""Mais Retorno Available Funds Paginated Listing."""

from stpstone.ingestion.countries.br.registries.mais_retorno_avl_funds import (
	MaisRetornoAvlFunds,
)


cls_ = MaisRetornoAvlFunds(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF MAIS RETORNO AVAILABLE FUNDS: \n{df_}")
df_.info()
