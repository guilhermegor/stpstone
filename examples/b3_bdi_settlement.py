"""B3 BDI OTC Settlement volumes ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_settlement import B3BdiSettlement


cls_ = B3BdiSettlement(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI SETTLEMENT: \n{df_}")
df_.info()
