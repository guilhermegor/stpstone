"""B3 BDI stocks traded strategies ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_traded_strategies import (
    B3BdiStocksTradedStrategies,
)


cls_ = B3BdiStocksTradedStrategies(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Stocks Traded Strategies: \n{df_}")
df_.info()
