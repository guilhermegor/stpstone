"""B3 BDI fixed income securities trades on the Puma platform."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_fixed_income_puma_trades import (
	B3BdiFixedIncomePumaTrades,
)


cls_ = B3BdiFixedIncomePumaTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
	int_page_min=1,
	int_page_max=5,
)

df_ = cls_.run()
print(f"DF B3 BDI FIXED INCOME PUMA TRADES: \n{df_}")
df_.info()
