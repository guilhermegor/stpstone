"""BCB Olinda annual market expectations for economic indicators."""

from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_annual_market_expectations import (
    BCBOlindaAnnualMarketExpectations,
)


cls_ = BCBOlindaAnnualMarketExpectations(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF BCB OLINDA ANNUAL MARKET EXPECTATIONS: \n{df_}")
df_.info()
