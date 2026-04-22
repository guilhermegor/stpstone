"""B3 BDI earnings credited for variable income securities ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_securities_earnings import (
	B3BdiSecuritiesEarnings,
)


cls_ = B3BdiSecuritiesEarnings(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Securities Earnings: \n{df_}")
df_.info()
