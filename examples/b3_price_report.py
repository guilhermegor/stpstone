"""B3 price report with traded quantities and best bid/ask prices."""

from stpstone.ingestion.countries.br.exchange.b3_price_report import B3PriceReport


cls_ = B3PriceReport(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 PRICE REPORT: \n{df_}")
df_.info()
