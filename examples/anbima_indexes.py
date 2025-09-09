"""Anbima Indexes."""

from stpstone.ingestion.countries.br.exchange.anbima_indexes import (
    AnbimaIndexesMTM,
    AnbimaIndexesPortfComp,
)


cls_ = AnbimaIndexesMTM(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA INDEXES MTM: \n{df_}")
df_.info


cls_ = AnbimaIndexesPortfComp(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA INDEXES PORTF COMP: \n{df_}")
df_.info