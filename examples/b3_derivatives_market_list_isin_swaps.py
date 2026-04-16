"""B3 derivatives market list of ISIN codes for swap contracts."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_list_isin_swaps import (
	B3DerivativesMarketListISINSwaps,
)


cls_ = B3DerivativesMarketListISINSwaps(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET LIST I S I N SWAPS: \n{df_}")
df_.info()
