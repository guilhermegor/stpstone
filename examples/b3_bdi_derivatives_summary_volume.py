"""B3 BDI derivatives daily average volume summary by period."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_summary_volume import (
	B3BdiDerivativesSummaryVolume,
)


cls_ = B3BdiDerivativesSummaryVolume(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI DERIVATIVES SUMMARY VOLUME: \n{df_}")
df_.info()
