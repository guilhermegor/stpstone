"""Anbima Debentures available listing data."""

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_available import (
	AnbimaDataDebenturesAvailable,
)


cls_ = AnbimaDataDebenturesAvailable(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA DEBENTURES AVAILABLE: \n{df_}")
df_.info()
