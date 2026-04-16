"""B3 Trading Hours for Stock Index Futures."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_stock_index_futures import (
	B3TradingHoursStockIndexFutures,
)


cls_ = B3TradingHoursStockIndexFutures(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS STOCK INDEX FUTURES: \n{df_}")
df_.info()
