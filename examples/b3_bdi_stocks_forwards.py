"""B3 BDI Forward Market stocks ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_forwards import B3BdiStocksForwards


cls_ = B3BdiStocksForwards(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Stocks Forwards: \n{df_}")
df_.info()
