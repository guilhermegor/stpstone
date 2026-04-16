"""Anbima Data IDKA Pre 1 Ano historical index series from ANBIMA S3."""

from stpstone.ingestion.countries.br.exchange.anbima_data_idka_pre_1a import AnbimaDataIDKAPre1A


cls_ = AnbimaDataIDKAPre1A(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IDKA Pre 1 Ano: \n{df_}")
df_.info()
