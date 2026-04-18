"""B3 BDI DI Over overnight interbank deposit rate."""

from stpstone.ingestion.countries.br.macroeconomics.b3_bdi_fixed_income_di_over import B3BdiFixedIncomeDiOver


cls_ = B3BdiFixedIncomeDiOver(
	date_ref=None,
	logger=None,
	cls_db=None,
	int_page_min=1,
	int_page_max=2,
)

df_ = cls_.run()
print(f"DF B3 BDI DI OVER: \n{df_}")
df_.info()
