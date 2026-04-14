"""B3 Trading Hours for Commodities Futures."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_commodities_futures import (
	B3TradingHoursCommoditiesFutures,
)


cls_ = B3TradingHoursCommoditiesFutures(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS COMMODITIES FUTURES: \n{df_}")
df_.info()
