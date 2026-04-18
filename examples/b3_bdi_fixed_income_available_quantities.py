"""B3 BDI daily register of fixed-income instruments — available quantities by type."""

from stpstone.ingestion.countries.br.registries.b3_bdi_fixed_income_available_quantities import (
	B3BdiFixedIncomeAvailableQuantities,
)


cls_ = B3BdiFixedIncomeAvailableQuantities(
	date_ref=None,
	logger=None,
	cls_db=None,
	int_page_min=1,
	int_page_max=10,
)

df_ = cls_.run()
print(f"DF B3 BDI FIXED INCOME AVAILABLE QUANTITIES: \n{df_}")
df_.info()
