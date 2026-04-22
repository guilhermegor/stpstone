"""B3 BDI securities lending contract renewals ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_btb_renewals import (
	B3BdiBtbRenewals,
)


cls_ = B3BdiBtbRenewals(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI BTB Renewals: \n{df_}")
df_.info()
