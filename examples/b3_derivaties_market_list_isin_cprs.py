"""B3 derivatives market list of ISIN codes for CPRs."""

from stpstone.ingestion.countries.br.exchange.b3_derivaties_market_list_isin_cprs import (
	B3DerivatiesMarketListISINCPRs,
)


cls_ = B3DerivatiesMarketListISINCPRs(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DERIVATIVES MARKET LIST I S I N C P RS: \n{df_}")
df_.info()
