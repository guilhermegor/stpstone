"""B3 Trading Hours for Exercise and Blocking of Options Before Exercise Date."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_exercise_blocking_options_before_exercise_date import (
	B3TradingHoursExerciseBlockingOptionsBeforeExerciseDate,
)


cls_ = B3TradingHoursExerciseBlockingOptionsBeforeExerciseDate(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS EXERCISE BLOCKING OPTIONS BEFORE EXERCISE DATE: \n{df_}")
df_.info()
