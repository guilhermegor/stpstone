"""B3 BDI index portfolio composition preview (PreviaQuadrimestral) ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_composition_portfolios_preview import (  # noqa: E501
	B3BdiIndexesCompositionPortfoliosPreview,
)


cls_ = B3BdiIndexesCompositionPortfoliosPreview(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Indexes Composition Portfolios Preview: \n{df_}")
df_.info()
