"""B3 BDI fungible custody securities subscription deadlines ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_securities_fungible_custody import (
	B3BdiSecuritiesFungibleCustody,
)


cls_ = B3BdiSecuritiesFungibleCustody(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Securities Fungible Custody: \n{df_}")
df_.info()
