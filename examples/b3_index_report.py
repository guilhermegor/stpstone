"""B3 index report with open, close, high, low and settlement values."""

from stpstone.ingestion.countries.br.exchange.b3_index_report import B3IndexReport


cls_ = B3IndexReport(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 INDEX REPORT: \n{df_}")
df_.info()
