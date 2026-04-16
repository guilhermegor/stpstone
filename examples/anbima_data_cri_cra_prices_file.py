"""Anbima CRI/CRA prices data via file download."""

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_prices_file import (
	AnbimaDataCRICRAPricesFile,
)


cls_ = AnbimaDataCRICRAPricesFile(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA CRI/CRA PRICES FILE: \n{df_}")
df_.info()
