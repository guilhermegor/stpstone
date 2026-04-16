"""B3 Trading Hours for OTC (Balcao Organizado)."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_otc import B3TradingHoursOTC


cls_ = B3TradingHoursOTC(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS OTC: \n{df_}")
df_.info()
