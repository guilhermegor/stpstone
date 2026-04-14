"""B3 derivatives market swap market rates by vertex."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_swap_market_rates import (
	B3DerivativesMarketSwapMarketRates,
)


cls_ = B3DerivativesMarketSwapMarketRates(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET SWAP MARKET RATES: \n{df_}")
df_.info()
