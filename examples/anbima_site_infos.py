"""Anbima site infos."""

from stpstone.ingestion.countries.br.exchange.anbima_site_infos import (
    AnbimaExchangeBRIMAP2PVs,
    AnbimaExchangeInfosBRCorporateBonds,
    AnbimaExchangeInfosBRTreasuries,
    AnbimaIMAP2TheoreticalPortfolio,
)


cls_ = AnbimaExchangeInfosBRTreasuries(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA EXCHANGE INFOS BR TREASURIES: \n{df_}")
df_.info()


cls_ = AnbimaExchangeInfosBRCorporateBonds(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA EXCHANGE INFOS BR CORPORATE BONDS: \n{df_}")
df_.info()


cls_ = AnbimaExchangeBRIMAP2PVs(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA EXCHANGE INFOS BR IMAP2 PVs: \n{df_}")
df_.info()


cls_ = AnbimaIMAP2TheoreticalPortfolio(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA IMAP2 THEORETICAL PORTFOLIO: \n{df_}")
df_.info()