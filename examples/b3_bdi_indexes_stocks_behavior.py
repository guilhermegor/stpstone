"""B3 BDI Indexes - any index stocks behavior (highs, lows, stable counts, year extremes)."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_stocks_behavior import (
	B3BdiIndexesStocksBehavior,
)


cls_ = B3BdiIndexesStocksBehavior(
	str_index_name="IBOVESPA",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI INDEXES ACTIONS BEHAVIOR (IBOVESPA): \n{df_}")
df_.info()
