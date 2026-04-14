"""B3 variable fees schedule for derivatives and equities trading."""

from datetime import date

from stpstone.ingestion.countries.br.exchange.b3_variable_fees import B3VariableFees


cls_ = B3VariableFees(
	date_ref=date(2025, 9, 1),
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 VARIABLE FEES: \n{df_}")
df_.info()
