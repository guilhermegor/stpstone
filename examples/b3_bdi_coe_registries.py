"""B3 BDI COE registration ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_coe_registries import (
	B3BdiCoeRegistries,
)


cls_ = B3BdiCoeRegistries(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI COE Registries: \n{df_}")
df_.info()
