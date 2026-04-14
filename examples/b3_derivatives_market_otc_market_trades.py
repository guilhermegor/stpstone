"""B3 derivatives market OTC market trades in fixed-width format."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_otc_market_trades import (
	B3DerivativesMarketOTCMarketTrades,
)


cls_ = B3DerivativesMarketOTCMarketTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET O T C MARKET TRADES: \n{df_}")
df_.info()
