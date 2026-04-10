"""Mais Retorno Fund Detail Properties."""

from stpstone.ingestion.countries.br.registries.mais_retorno_fund_properties import (
	MaisRetornoFundProperties,
)


cls_ = MaisRetornoFundProperties(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF MAIS RETORNO FUND PROPERTIES: \n{df_}")
df_.info()
