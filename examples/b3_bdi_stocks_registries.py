"""B3 BDI Instruments Equities registration - instrument details for B3-listed equities."""

from stpstone.ingestion.countries.br.registries.b3_bdi_stocks_registries import (
	B3BdiStocksRegistries,
)


cls_ = B3BdiStocksRegistries(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI STOCKS REGISTRIES: \n{df_}")
df_.info()
