"""Anbima Indexes Market-to-Market data."""

from stpstone.ingestion.countries.br.exchange.anbima_indexes_mtm import AnbimaIndexesMTM


cls_ = AnbimaIndexesMTM(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF ANBIMA INDEXES MTM: \n{df_}")
df_.info()
