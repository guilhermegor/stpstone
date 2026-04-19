"""B3 BDI Fixed Income Land Reform Bonds - TDA nominal values by bond type."""

from stpstone.ingestion.countries.br.registries.b3_bdi_fixed_income_land_reform_bonds import (
	B3BdiFixedIncomeLandReformBonds,
)


cls_ = B3BdiFixedIncomeLandReformBonds(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
if df_ is not None:
	print(f"DF B3 BDI FIXED INCOME LAND REFORM BONDS: \n{df_}")
	df_.info()
else:
	print("No data available — TDA is published on the 5th business day of each month.")
