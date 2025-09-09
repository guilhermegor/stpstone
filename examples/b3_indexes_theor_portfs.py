"""Example of ingestion of B3 Theorical Portfolio data."""

from stpstone.ingestion.countries.br.exchange.b3_indexes_theor_portfs import (
    B3TheoricalPortfolioIBOV,
    B3TheoricalPortfolioIBRA,
    B3TheoricalPortfolioIBRX50,
)


cls_ = B3TheoricalPortfolioIBOV(
    date_ref=None, 
    logger=None,
    cls_db=None,
)
df_ = cls_.run()
print(f"DF B3 THEORICAL PORTFOLIO IBOV: \n{df_}")
df_.info()


cls_ = B3TheoricalPortfolioIBRA(
    date_ref=None, 
    logger=None,
    cls_db=None,
)
df_ = cls_.run()
print(f"DF B3 THEORICAL PORTFOLIO IBRA: \n{df_}")
df_.info()


cls_ = B3TheoricalPortfolioIBRX50(
    date_ref=None, 
    logger=None,
    cls_db=None,
)
df_ = cls_.run()
print(f"DF B3 THEORICAL PORTFOLIO IBRX50: \n{df_}")
df_.info()