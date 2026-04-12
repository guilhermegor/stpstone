"""Anbima Data IDA LIQ Geral historical index series from ANBIMA S3."""

from stpstone.ingestion.countries.br.exchange.anbima_data_ida_liq_geral import (
	AnbimaDataIDALIQGeral,
)


cls_ = AnbimaDataIDALIQGeral(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IDA LIQ Geral: \n{df_}")
df_.info()
