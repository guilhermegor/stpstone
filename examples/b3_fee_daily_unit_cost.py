"""B3 daily unit cost fee schedules for exchange and registration."""

from stpstone.ingestion.countries.br.exchange.b3_fee_daily_unit_cost import B3FeeDailyUnitCost


cls_ = B3FeeDailyUnitCost(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 FEE DAILY UNIT COST: \n{df_}")
df_.info()
