"""Anbima CRI/CRA search listing with characteristics data."""

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_characteristics import (
	AnbimaDataCRICRACharacteristics,
)


cls_ = AnbimaDataCRICRACharacteristics(
	date_ref=None,
	logger=None,
	cls_db=None,
	start_page=0,
	end_page=None,
)

df_ = cls_.run()
print(f"DF ANBIMA CRI/CRA CHARACTERISTICS: \n{df_}")
df_.info()
