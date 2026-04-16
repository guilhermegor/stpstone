"""Anbima Debentures characteristics data."""

from stpstone.ingestion.countries.br.registries.anbima_data_debentures_characteristics import (
	AnbimaDataDebenturesCharacteristics,
)


cls_ = AnbimaDataDebenturesCharacteristics(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA DEBENTURES CHARACTERISTICS: \n{df_}")
df_.info()
