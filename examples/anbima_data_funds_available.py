"""Anbima Data Funds Available — listing of all funds available on ANBIMA Data portal."""

from stpstone.ingestion.countries.br.registries.anbima_data_funds_available import (
	AnbimaDataFundsAvailable,
)


cls_ = AnbimaDataFundsAvailable(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA DATA FUNDS AVAILABLE: \n{df_}")
df_.info()
