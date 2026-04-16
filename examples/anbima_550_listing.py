"""Anbima 550 Listing."""

from stpstone.ingestion.countries.br.exchange.anbima_550_listing import Anbima550Listing


cls_ = Anbima550Listing(date_ref=None, logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF ANBIMA 550 LISTING: \n{df_}")
df_.info()
