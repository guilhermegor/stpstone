"""B3 BDI IBOVESPA equities with the largest percentage gains ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_ibov_greatest_upsides import (
    B3BdiIndexesIbovGreatestUpsides,
)


cls_ = B3BdiIndexesIbovGreatestUpsides(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Indexes IBOV Greatest Upsides: \n{df_}")
df_.info()
