"""B3 BDI OTC Position Inventory."""

from stpstone.ingestion.countries.br.otc.b3_bdi_inventory import B3BdiInventory


cls_ = B3BdiInventory(
	date_ref=None,
	logger=None,
	cls_db=None,
	int_page_min=1,
	int_page_max=2,
)

df_ = cls_.run()
print(f"DF BDI OTC POSITION INVENTORY: \n{df_}")
df_.info()
