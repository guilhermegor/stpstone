"""B3 BDI Derivatives - Conilon coffee certified lots crop statistics."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_crop_statistics_coffee_conilon import (
	B3BdiDerivativesCropStatisticsCoffeeConilon,
)


cls_ = B3BdiDerivativesCropStatisticsCoffeeConilon(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
if df_ is not None:
	print(f"DF B3 BDI DERIVATIVES CROP STATISTICS CONILON COFFEE: \n{df_}")
	df_.info()
else:
	print("No data available for the reference date.")
