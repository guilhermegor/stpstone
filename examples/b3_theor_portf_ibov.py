"""B3 IBOV theoretical portfolio ingestion example."""

from stpstone.ingestion.countries.br.exchange.b3_theor_portf_ibov import B3TheoricalPortfolioIBOV


cls_ = B3TheoricalPortfolioIBOV(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 THEORICAL PORTFOLIO IBOV: \n{df_}")
df_.info()
