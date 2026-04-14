"""B3 Trading Hours for US Dollar Denominated Interest Rates Futures."""

from stpstone.ingestion.countries.br.exchange.b3_trading_hours_usdollar_denominated_interest_rates_futures import (
	B3TradingHoursUSDollarDenominatedInterestRatesFutures,
)


cls_ = B3TradingHoursUSDollarDenominatedInterestRatesFutures(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 TRADING HOURS USDOLLAR DENOMINATED INTEREST RATES FUTURES: \n{df_}")
df_.info()
