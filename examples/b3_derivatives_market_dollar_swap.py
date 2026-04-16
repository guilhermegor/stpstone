"""B3 derivatives market dollar swap data in fixed-width format."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_dollar_swap import (
	B3DerivativesMarketDollarSwap,
)


cls_ = B3DerivativesMarketDollarSwap(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET DOLLAR SWAP: \n{df_}")
df_.info()
