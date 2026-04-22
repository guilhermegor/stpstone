"""B3 BDI most traded stocks (cash market) ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_most_traded import (
    B3BdiStocksMostTraded,
)


cls_ = B3BdiStocksMostTraded(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Stocks Most Traded: \n{df_}")
df_.info()
