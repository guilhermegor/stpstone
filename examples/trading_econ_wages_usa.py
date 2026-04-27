"""Trading Economics US wages and related compensation indicators."""

from stpstone.ingestion.countries.ww.macroeconomics.trading_econ_wages_usa import (
	TradingEconWagesUsa,
)


cls_ = TradingEconWagesUsa(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF TRADING ECONOMICS WAGES USA: \n{df_}")
df_.info()
