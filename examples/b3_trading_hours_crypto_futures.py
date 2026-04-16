"""B3 Trading Hours for Crypto Futures."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_crypto_futures import (
	B3TradingHoursCryptoFutures,
)


cls_ = B3TradingHoursCryptoFutures(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS CRYPTO FUTURES: \n{df_}")
df_.info()
