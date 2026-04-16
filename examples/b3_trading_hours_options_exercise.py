"""B3 Trading Hours for Options Exercise."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_options_exercise import (
	B3TradingHoursOptionsExercise,
)


cls_ = B3TradingHoursOptionsExercise(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS OPTIONS EXERCISE: \n{df_}")
df_.info()
