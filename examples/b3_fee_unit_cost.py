"""B3 unit cost fee schedules for non-daily fee assessment."""

from datetime import date

from stpstone.ingestion.countries.br.exchange.b3_fee_unit_cost import B3FeeUnitCost


cls_ = B3FeeUnitCost(
	date_ref=date(2025, 9, 1),
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 FEE UNIT COST: \n{df_}")
df_.info()
