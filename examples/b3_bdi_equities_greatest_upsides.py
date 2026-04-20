"""B3 BDI cash market equities with the largest gains ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_equities_greatest_upsides import (
    B3BdiEquitiesGreatestUpsides,
)


cls_ = B3BdiEquitiesGreatestUpsides(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Equities Greatest Upsides: \n{df_}")
df_.info()
