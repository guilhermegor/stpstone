"""B3 securities market government bond PU prices for stress scenarios."""

from stpstone.ingestion.countries.br.exchange.b3_securities_market_government_securities_prices import (
	B3SecuritiesMarketGovernmentSecuritiesPrices,
)


cls_ = B3SecuritiesMarketGovernmentSecuritiesPrices(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 SECURITIES MARKET GOVERNMENT SECURITIES PRICES: \n{df_}")
df_.info()
