"""Anbima Data IDA Geral historical index series from ANBIMA S3."""

from stpstone.ingestion.countries.br.exchange.anbima_data_ida_geral import AnbimaDataIDAGeral


cls_ = AnbimaDataIDAGeral(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IDA Geral: \n{df_}")
df_.info()
