"""B3 Trading Hours for Exchange Rate Futures."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_exchange_rate_futures import (
	B3TradingHoursExchangeRateFutures,
)


cls_ = B3TradingHoursExchangeRateFutures(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS EXCHANGE RATE FUTURES: \n{df_}")
df_.info()
