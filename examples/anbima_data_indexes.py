"""Anbima Data Indexes historical series from ANBIMA S3."""

from stpstone.ingestion.countries.br.exchange.anbima_data_indexes import AnbimaDataIMAGeral


cls_ = AnbimaDataIMAGeral(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IMA Geral: \n{df_}")
df_.info()
