"""B3 Trading Hours for Stocks Market."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_stocks import B3TradingHoursStocks


cls_ = B3TradingHoursStocks(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS STOCKS: \n{df_}")
df_.info()
