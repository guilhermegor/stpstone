"""B3 BDI OTC derivatives inventory with CCP ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_ccp import (
	B3BdiDerivativesInventoryCcp,
)


cls_ = B3BdiDerivativesInventoryCcp(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Inventory CCP: \n{df_}")
df_.info()
