"""Brazilian IRS (Receita Federal) Simplified Taxation System (Simples) Open Data."""

from stpstone.ingestion.countries.br.taxation.irsbr_simplified_taxation import (
	IRSBRSimplifiedTaxation,
)


cls_ = IRSBRSimplifiedTaxation(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF IRSBR SIMPLIFIED TAXATION: \n{df_}")
df_.info()
