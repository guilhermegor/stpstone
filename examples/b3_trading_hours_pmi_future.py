"""B3 Trading Hours for PMI Future."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_pmi_future import (
	B3TradingHoursPMIFuture,
)


cls_ = B3TradingHoursPMIFuture(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS PMI FUTURE: \n{df_}")
df_.info()
