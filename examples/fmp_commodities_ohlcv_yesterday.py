"""FMP batch commodity OHLCV quotes for the previous trading day."""

from stpstone.ingestion.countries.ww.exchange.markets.fmp_commodities_ohlcv_yesterday import (
	FMPCommoditiesOhlcvYesterday,
)


cls_ = FMPCommoditiesOhlcvYesterday(
	token="YOUR_FMP_TOKEN",
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF FMP Commodities OHLCV Yesterday: \n{df_}")
df_.info()
