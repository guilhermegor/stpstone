"""B3 BDI stocks trade-by-trade ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_trade_by_trade import (
	B3BdiStocksTradeByTrade,
)


cls_ = B3BdiStocksTradeByTrade(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI STOCKS TRADE BY TRADE: \n{df_}")
df_.info()
