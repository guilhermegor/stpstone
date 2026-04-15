"""FMP batch stock OHLCV quotes for the previous trading day."""

from stpstone.ingestion.countries.ww.exchange.markets.fmp_stocks_ohlcv_yesterday import (
	FMPStocksOhlcvYesterday,
)


cls_ = FMPStocksOhlcvYesterday(
	token="YOUR_FMP_TOKEN",
	list_slugs=["AAPL", "MSFT", "GOOGL"],
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF FMP Stocks OHLCV Yesterday: \n{df_}")
df_.info()
