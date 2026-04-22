"""B3 BDI OTC Settlement volumes ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_fixed_income_settlements import B3BdiFixedIncomeSettlements


cls_ = B3BdiFixedIncomeSettlements(
	date_ref=None,
	logger=None,
	cls_db=None,
    int_page_min=1,
	int_page_max=5,
)

df_ = cls_.run()
print(f"DF B3 BDI SETTLEMENT: \n{df_}")
df_.info()
