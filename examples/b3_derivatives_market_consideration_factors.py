"""B3 derivatives market consideration factors per primitive risk factor."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_consideration_factors import (
	B3DerivativesMarketConsiderationFactors,
)


cls_ = B3DerivativesMarketConsiderationFactors(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET CONSIDERATION FACTORS: \n{df_}")
df_.info()
