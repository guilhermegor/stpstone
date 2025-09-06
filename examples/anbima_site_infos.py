"""Anbima site infos."""

from stpstone.ingestion.countries.br.exchange.anbima_site_infos import AnbimaExchangeInfos


cls_ = AnbimaExchangeInfos(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA 550 LISTING: \n{df_}")
df_.info()