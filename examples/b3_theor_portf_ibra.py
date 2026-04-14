"""B3 IBRA theoretical portfolio ingestion example."""

from stpstone.ingestion.countries.br.exchange.b3_theor_portf_ibra import B3TheoricalPortfolioIBRA


cls_ = B3TheoricalPortfolioIBRA(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 THEORICAL PORTFOLIO IBRA: \n{df_}")
df_.info()
