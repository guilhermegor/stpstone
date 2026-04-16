"""B3 IBRX50 theoretical portfolio ingestion example."""

from stpstone.ingestion.countries.br.exchange.b3_theor_portf_ibrx50 import (
	B3TheoricalPortfolioIBRX50,
)


cls_ = B3TheoricalPortfolioIBRX50(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 THEORETICAL PORTFOLIO IBRX50: \n{df_}")
df_.info()
