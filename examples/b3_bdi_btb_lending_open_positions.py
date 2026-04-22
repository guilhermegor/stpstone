"""B3 BDI securities lending open positions ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_btb_lending_open_positions import (
	B3BdiBtbLendingOpenPositions,
)


cls_ = B3BdiBtbLendingOpenPositions(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI BTB Lending Open Positions: \n{df_}")
df_.info()
