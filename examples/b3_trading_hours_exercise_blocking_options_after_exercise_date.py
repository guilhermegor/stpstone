"""B3 Trading Hours for Exercise and Blocking of Options After Exercise Date."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_exercise_blocking_options_after_exercise_date import (
	B3TradingHoursExerciseBlockingOptionsAfterExerciseDate,
)


cls_ = B3TradingHoursExerciseBlockingOptionsAfterExerciseDate(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS EXERCISE BLOCKING OPTIONS AFTER EXERCISE DATE: \n{df_}")
df_.info()
