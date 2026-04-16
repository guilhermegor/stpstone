"""Tiingo US daily adjusted OHLCV data."""

from stpstone.ingestion.countries.us.exchange.tiingo import TiingoUS


cls_ = TiingoUS(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF TIINGO: \n{df_}")
df_.info()
