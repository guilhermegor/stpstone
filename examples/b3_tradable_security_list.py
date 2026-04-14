"""B3 tradable security list with all currently eligible instruments."""

from stpstone.ingestion.countries.br.exchange.b3_tradable_security_list import (
	B3TradableSecurityList,
)


cls_ = B3TradableSecurityList(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADABLE SECURITY LIST: \n{df_}")
df_.info()
