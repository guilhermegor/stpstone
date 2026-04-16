"""FMP cryptocurrency OHLCV quotes for the previous trading day."""

from stpstone.ingestion.countries.ww.exchange.markets.fmp_crypto_ohlcv_yesterday import (
	FMPCryptoOhlcvYesterday,
)


cls_ = FMPCryptoOhlcvYesterday(
	token="YOUR_FMP_TOKEN",
	date_ref=None,
	list_slugs=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF FMP Crypto OHLCV Yesterday: \n{df_}")
df_.info()
