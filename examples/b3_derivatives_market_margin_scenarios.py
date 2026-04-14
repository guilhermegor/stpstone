"""B3 derivatives market margin scenarios for liquid assets."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_margin_scenarios import (
	B3DerivativesMarketMarginScenarios,
)


cls_ = B3DerivativesMarketMarginScenarios(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET MARGIN SCENARIOS: \n{df_}")
df_.info()
