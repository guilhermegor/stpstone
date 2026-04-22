"""B3 BDI Derivatives - Type 4/5 coffee production batch formation centers."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_production_batch_formation_45_coffee import (
	B3BdiDerivativesProductionBatchFormation45Coffee,
)


cls_ = B3BdiDerivativesProductionBatchFormation45Coffee(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI DERIVATIVES PRODUCTION BATCH FORMATION 4/5 COFFEE: \n{df_}")
df_.info()
