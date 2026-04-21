"""B3 BDI OTC electronic forward contracts ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_electronic_forwards import (
	B3BdiDerivativesElectronicForwards,
)


cls_ = B3BdiDerivativesElectronicForwards(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Electronic Forwards: \n{df_}")
if df_ is not None:
	df_.info()
