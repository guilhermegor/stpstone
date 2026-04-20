"""B3 BDI OTC derivatives inventory without CCP ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_derivatives_inventory_wccp import (
	B3BdiDerivativesInventoryWccp,
)


cls_ = B3BdiDerivativesInventoryWccp(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Derivatives Inventory WCCP: \n{df_}")
df_.info()
