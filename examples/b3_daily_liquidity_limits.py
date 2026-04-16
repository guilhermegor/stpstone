"""B3 daily liquidity limits per instrument group and clearing house."""

from stpstone.ingestion.countries.br.exchange.b3_daily_liquidity_limits import (
	B3DailyLiquidityLimits,
)


cls_ = B3DailyLiquidityLimits(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 DAILY LIQUIDITY LIMITS: \n{df_}")
df_.info()
