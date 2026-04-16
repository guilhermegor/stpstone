"""ANBIMA IMA-P2 theoretical portfolio composition data."""

from stpstone.ingestion.countries.br.exchange.anbima_ima_p2_portf_comp import (
	AnbimaIMAP2TheoreticalPortfolio,
)


cls_ = AnbimaIMAP2TheoreticalPortfolio(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA IMA P2 THEORETICAL PORTFOLIO: \n{df_}")
df_.info()
