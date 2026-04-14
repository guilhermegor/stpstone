"""B3 derivatives market list of ISIN codes for derivatives contracts."""

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_list_isin_derivatives_contracts import (
	B3DerivativesMarketListISINDerivativesContracts,
)


cls_ = B3DerivativesMarketListISINDerivativesContracts(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET LIST I S I N DERIVATIVES CONTRACTS: \n{df_}")
df_.info()
