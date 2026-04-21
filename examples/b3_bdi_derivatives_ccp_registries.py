"""B3 BDI OTC derivatives registration with CCP ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_ccp_registries import (
	B3BdiDerivativesCcpRegistries,
)


cls_ = B3BdiDerivativesCcpRegistries(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives CCP Registries: \n{df_}")
df_.info()
