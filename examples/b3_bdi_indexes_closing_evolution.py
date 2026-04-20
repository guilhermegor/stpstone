"""B3 BDI Indexes - any index closing evolution (day, week, month, year returns)."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_closing_evolution import (
	B3BdiIndexesClosingEvolution,
)


cls_ = B3BdiIndexesClosingEvolution(
	str_index_name="IBOVESPA",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI INDEXES CLOSING EVOLUTION (IBOVESPA): \n{df_}")
df_.info()
