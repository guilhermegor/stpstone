"""B3 BDI Repo Debenture Transactions."""

from stpstone.ingestion.countries.br.otc.b3_bdi_fixed_income_debentures_repurchase_agreements import (
	B3BdiFixedIncomeDebenturesRepurchaseAgreements,
)


cls_ = B3BdiFixedIncomeDebenturesRepurchaseAgreements(
	date_ref=None,
	logger=None,
	cls_db=None,
    int_page_min=1,
	int_page_max=2,
)

df_ = cls_.run()
print(f"DF REPO DEBENTURE TRANSACTIONS: \n{df_}")
df_.info()
