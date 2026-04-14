"""B3 fixed income securities prices with buy/sell indicative rates."""

from stpstone.ingestion.countries.br.exchange.b3_fixed_income import B3FixedIncome


cls_ = B3FixedIncome(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 FIXED INCOME: \n{df_}")
df_.info()
