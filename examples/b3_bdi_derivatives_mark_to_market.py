"""B3 BDI derivatives mark-to-market settlement prices and open interest."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_mark_to_market import (
	B3BdiDerivativesMarkToMarket,
)


cls_ = B3BdiDerivativesMarkToMarket(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI DERIVATIVES MARK TO MARKET: \n{df_}")
df_.info()
