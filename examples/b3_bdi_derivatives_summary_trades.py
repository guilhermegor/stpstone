"""B3 BDI derivatives transactions summary by instrument, market, and contract type."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_derivatives_summary_trades import (
	B3BdiDerivativesSummaryTrades,
)


cls_ = B3BdiDerivativesSummaryTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI DERIVATIVES SUMMARY TRADES: \n{df_}")
df_.info()
