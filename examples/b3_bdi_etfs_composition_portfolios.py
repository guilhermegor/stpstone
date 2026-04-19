"""B3 BDI index portfolio composition (PreviaQuadrimestral) ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_etfs_composition_portfolios import (
	B3BdiEtfsCompositionPortfolios,
)


cls_ = B3BdiEtfsCompositionPortfolios(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI ETFs Composition Portfolios: \n{df_}")
df_.info()
