"""B3 BDI Indexes - any index day behavior (open, min, max, close, averages)."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_day_behavior import (
	B3BdiIndexesDayBehavior,
)


cls_ = B3BdiIndexesDayBehavior(
	str_index_name="IBOVESPA",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI INDEXES DAY BEHAVIOR (IBOVESPA): \n{df_}")
df_.info()
