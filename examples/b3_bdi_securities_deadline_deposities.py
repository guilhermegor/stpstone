"""B3 BDI deadline for deposit of securities ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_securities_deadline_deposities import (
	B3BdiSecuritiesDeadlineDeposities,
)


cls_ = B3BdiSecuritiesDeadlineDeposities(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Securities Deadline Deposities: \n{df_}")
df_.info()
