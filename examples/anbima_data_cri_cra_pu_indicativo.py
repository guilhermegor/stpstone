"""Anbima CRI/CRA indicative unit price data."""

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_pu_indicativo import (
	AnbimaDataCRICRAPUIndicativo,
)


cls_ = AnbimaDataCRICRAPUIndicativo(
	date_ref=None,
	logger=None,
	cls_db=None,
	list_asset_codes=["18L1085826", "19C0000001", "CRA019000GT"],
)

df_ = cls_.run()
print(f"DF ANBIMA CRI/CRA PU INDICATIVO: \n{df_}")
df_.info()
