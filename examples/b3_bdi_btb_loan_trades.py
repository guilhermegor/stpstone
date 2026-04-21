"""B3 BDI securities lending trades ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_btb_loan_trades import (
	B3BdiBtbLoanTrades,
)


cls_ = B3BdiBtbLoanTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI BTB Loan Trades: \n{df_}")
df_.info()
