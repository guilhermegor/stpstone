"""Anbima Debentures prices data."""

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_prices import (
	AnbimaDataDebenturesPrices,
)


cls_ = AnbimaDataDebenturesPrices(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA DEBENTURES PRICES: \n{df_}")
df_.info()
