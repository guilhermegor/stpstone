"""B3 BDI OTC derivatives registration without CCP ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_wccp_registries import (
	B3BdiDerivativesWccpRegistries,
)


cls_ = B3BdiDerivativesWccpRegistries(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives WCCP Registries: \n{df_}")
df_.info()
