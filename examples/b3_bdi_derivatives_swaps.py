"""B3 BDI OTC flexible swap contracts ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_swaps import (
	B3BdiDerivativesSwaps,
)


cls_ = B3BdiDerivativesSwaps(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Swaps: \n{df_}")
if df_ is not None:
	df_.info()
