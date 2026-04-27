"""Anbima Data Funds Historic — periodic historical data for individual funds on ANBIMA Data portal."""

from stpstone.ingestion.countries.br.registries.anbima_data_funds_historic import (
	AnbimaDataFundsHistoric,
)


cls_ = AnbimaDataFundsHistoric(
	date_ref=None,
	logger=None,
	cls_db=None,
	list_fund_codes=["S0000634344"],
)

df_ = cls_.run()
print(f"DF ANBIMA DATA FUNDS HISTORIC: \n{df_}")
df_.info()
