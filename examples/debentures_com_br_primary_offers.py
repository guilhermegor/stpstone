"""Debentures.com.br primary offer volume by period."""

from stpstone.ingestion.countries.br.otc.debentures_com_br_primary_offers import (
    DebenturesComBrPrimaryOffers,
)


cls_ = DebenturesComBrPrimaryOffers(
    date_start=None,
    date_end=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF DEBENTURES COM BR PRIMARY OFFERS: \n{df_}")
df_.info()
