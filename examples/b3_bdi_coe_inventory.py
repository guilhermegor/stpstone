"""B3 BDI COE inventory ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_coe_inventory import B3BdiCoeInventory


cls_ = B3BdiCoeInventory(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI COE Inventory: \n{df_}")
df_.info()
