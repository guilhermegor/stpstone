"""Anbima CRI/CRA prices data via web scraping."""

from stpstone.ingestion.countries.br.registries.anbima_data_cri_cra_prices_ws import (
	AnbimaDataCRICRAPricesWS,
)


cls_ = AnbimaDataCRICRAPricesWS(
	date_ref=None,
	logger=None,
	cls_db=None,
	start_page=0,
	end_page=None,
)

df_ = cls_.run()
print(f"DF ANBIMA CRI/CRA PRICES WS: \n{df_}")
df_.info()
