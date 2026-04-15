"""FMP batch ETF OHLCV quotes for the previous trading day."""

from stpstone.ingestion.countries.ww.exchange.markets.fmp_etfs_ohlcv_yesterday import (
	FMPEtfsOhlcvYesterday,
)


cls_ = FMPEtfsOhlcvYesterday(
	token="YOUR_FMP_TOKEN",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF FMP ETFs OHLCV Yesterday: \n{df_}")
df_.info()
