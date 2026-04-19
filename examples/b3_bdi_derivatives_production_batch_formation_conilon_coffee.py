"""B3 BDI Derivatives - Conilon coffee production batch formation centers."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_production_batch_formation_conilon_coffee import (
	B3BdiDerivativesProductionBatchFormationConilonCoffee,
)


cls_ = B3BdiDerivativesProductionBatchFormationConilonCoffee(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
if df_ is not None:
	print(f"DF B3 BDI DERIVATIVES PRODUCTION BATCH FORMATION CONILON COFFEE: \n{df_}")
	df_.info()
else:
	print("No data available for the reference date.")
