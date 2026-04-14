"""B3 derivatives market economic and agricultural indicator values."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_economic_agricultural_indicators import (
	B3DerivativesMarketEconomicAgriculturalIndicators,
)


cls_ = B3DerivativesMarketEconomicAgriculturalIndicators(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET ECONOMIC AGRICULTURAL INDICATORS: \n{df_}")
df_.info()
