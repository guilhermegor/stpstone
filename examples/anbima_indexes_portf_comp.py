"""Anbima Indexes Portfolio Composition data."""

from stpstone.ingestion.countries.br.exchange.anbima_indexes_portf_comp import (
    AnbimaIndexesPortfComp,
)


cls_ = AnbimaIndexesPortfComp(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA INDEXES PORTF COMP: \n{df_}")
df_.info()
