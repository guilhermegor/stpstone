"""B3 BDI Consolidated Non-Exchange (After Market) trades ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_consolidated_non_exchange_trades import (
	B3BdiStocksConsolidatedNonExchangeTrades,
)


cls_ = B3BdiStocksConsolidatedNonExchangeTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Stocks Consolidated Non-Exchange Trades: \n{df_}")
df_.info()
