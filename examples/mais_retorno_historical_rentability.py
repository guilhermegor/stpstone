"""Mais Retorno Instruments Historical Monthly Rentability."""

from stpstone.ingestion.countries.br.registries.mais_retorno_historical_rentability import (
	MaisRetornoHistoricalRentability,
)


cls_ = MaisRetornoHistoricalRentability(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF MAIS RETORNO HISTORICAL RENTABILITY: \n{df_}")
df_.info()
