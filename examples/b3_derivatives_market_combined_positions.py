"""B3 derivatives market combined open positions by instrument."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_combined_positions import (
	B3DerivativesMarketCombinedPositions,
)


cls_ = B3DerivativesMarketCombinedPositions(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET COMBINED POSITIONS: \n{df_}")
df_.info()
