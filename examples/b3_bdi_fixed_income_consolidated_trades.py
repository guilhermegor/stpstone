"""B3 BDI consolidated tradings ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_fixed_income_consolidated_trades import (
	B3BdiFixedIncomeConsolidatedTrades,
)


cls_ = B3BdiFixedIncomeConsolidatedTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI CONSOLIDATED TRADINGS: \n{df_}")
df_.info()
