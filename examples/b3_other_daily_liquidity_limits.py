"""B3 other daily liquidity limits beyond standard instrument groups."""

from stpstone.ingestion.countries.br.exchange.b3_other_daily_liquidity_limits import (
	B3OtherDailyLiquidityLimits,
)


cls_ = B3OtherDailyLiquidityLimits(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 OTHER DAILY LIQUIDITY LIMITS: \n{df_}")
df_.info()
