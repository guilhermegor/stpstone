"""B3 BDI Daily Average Stocks - number of trades and volume by period."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_summary import B3BdiStocksSummary


cls_ = B3BdiStocksSummary(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI STOCKS SUMMARY: \n{df_}")
df_.info()
