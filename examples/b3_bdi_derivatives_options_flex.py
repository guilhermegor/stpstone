"""B3 BDI OTC flexible options ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_options_flex import (
	B3BdiDerivativesOptionsFlex,
)


cls_ = B3BdiDerivativesOptionsFlex(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Options Flex: \n{df_}")
df_.info()
