"""B3 BDI Fixed Income Trades ingestion."""

from stpstone.ingestion.countries.br.otc.b3_bdi_fixed_income_trades import B3BdiFixedIncomeTrades


cls_ = B3BdiFixedIncomeTrades(
	date_ref=None,
	logger=None,
	cls_db=None,
    int_page_min=1,
    int_page_max=5,
)

df_ = cls_.run()
print(f"DF B3 BDI FIXED INCOME TRADES: \n{df_}")
df_.info()
