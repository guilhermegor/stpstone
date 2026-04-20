"""B3 BDI IBOVESPA equities with the largest percentage losses ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_ibov_greatest_downsides import (
    B3BdiIndexesIbovGreatestDownsides,
)


cls_ = B3BdiIndexesIbovGreatestDownsides(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Indexes IBOV Greatest Downsides: \n{df_}")
df_.info()
