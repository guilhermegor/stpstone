"""B3 BDI securities lending registered loan balance ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_btb_loan_balance import (
	B3BdiBtbLoanBalance,
)


cls_ = B3BdiBtbLoanBalance(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI BTB Loan Balance: \n{df_}")
df_.info()
