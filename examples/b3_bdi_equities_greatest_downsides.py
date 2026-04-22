"""B3 BDI cash market equities with the largest losses ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_equities_greatest_downsides import (
    B3BdiEquitiesGreatestDownsides,
)


cls_ = B3BdiEquitiesGreatestDownsides(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Equities Greatest Downsides: \n{df_}")
df_.info()
