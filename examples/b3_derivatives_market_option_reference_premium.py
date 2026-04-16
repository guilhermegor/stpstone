"""B3 derivatives market option reference premiums and implied vol."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_option_reference_premium import (
	B3DerivativesMarketOptionReferencePremium,
)


cls_ = B3DerivativesMarketOptionReferencePremium(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET OPTION REFERENCE PREMIUM: \n{df_}")
df_.info()
