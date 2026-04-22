"""B3 BDI Derivatives - Arabica 4/5 coffee certified lots crop statistics."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_crop_statistics_coffee_45 import (
	B3BdiDerivativesCropStatisticsCoffee45,
)


cls_ = B3BdiDerivativesCropStatisticsCoffee45(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI DERIVATIVES CROP STATISTICS ARABICA 4/5 COFFEE: \n{df_}")
df_.info()
