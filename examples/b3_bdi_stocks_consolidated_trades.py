"""B3 BDI Consolidated Session Trades for equities ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_consolidated_trades import (
	B3BdiStocksConsolidatedTrades,
)


cls_ = B3BdiStocksConsolidatedTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Stocks Consolidated Trades: \n{df_}")
df_.info()
