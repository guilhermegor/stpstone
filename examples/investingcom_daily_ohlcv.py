"""Investing.com daily OHLCV history for worldwide exchange-traded instruments."""

from stpstone.ingestion.countries.ww.exchange.markets.investingcom_daily_ohlcv import (
	InvestingComDailyOhlcv,
)


cls_ = InvestingComDailyOhlcv(
	ticker_id=6408,
	str_ticker="PETR4",
	date_ref=None,
	date_start=None,
	date_end=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF INVESTING.COM DAILY OHLCV: \n{df_}")
df_.info()
