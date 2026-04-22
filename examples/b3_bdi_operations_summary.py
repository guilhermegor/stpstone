"""B3 BDI Equities - transactions summary by operation type."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_operations_summary import (
	B3BdiOperationsSummary,
)


cls_ = B3BdiOperationsSummary(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI STOCKS OPERATION SUMMARY: \n{df_}")
df_.info()
