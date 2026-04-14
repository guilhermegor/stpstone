"""B3 Trading Hours for Real Denominated Interest Rates."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_real_denominated_interest_rates import (
	B3TradingHoursRealDenominatedInterestRates,
)


cls_ = B3TradingHoursRealDenominatedInterestRates(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS REAL DENOMINATED INTEREST RATES: \n{df_}")
df_.info()
