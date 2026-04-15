"""Anbima CRI/CRA corporate events data."""

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_events import (
	AnbimaDataCRICRAEvents,
)


cls_ = AnbimaDataCRICRAEvents(
	date_ref=None,
	logger=None,
	cls_db=None,
	list_asset_codes=["18L1085826", "19C0000001", "CRA019000GT"],
)

df_ = cls_.run()
print(f"DF ANBIMA CRI/CRA EVENTS: \n{df_}")
df_.info()
