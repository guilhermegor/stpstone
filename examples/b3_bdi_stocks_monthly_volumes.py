"""B3 BDI stocks monthly volumes (daily averages annual) ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_stocks_monthly_volumes import (
    B3BdiStocksMonthlyVolumes,
)


cls_ = B3BdiStocksMonthlyVolumes(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Stocks Monthly Volumes: \n{df_}")
df_.info()
