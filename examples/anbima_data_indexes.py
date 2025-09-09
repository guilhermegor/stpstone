"""Anbima data indexes."""

from stpstone.ingestion.countries.br.exchange.anbima_data_indexes import (
    AnbimaDataIDAGeral,
    AnbimaDataIDALIQGeral,
    AnbimaDataIDKAPre1A,
    AnbimaDataIMAGeral,
)


cls_ = AnbimaDataIMAGeral(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA DATA INDEXES IMAGERAL: \n{df_}")
df_.info


cls_ = AnbimaDataIDAGeral(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA DATA INDEXES IDA GERAL: \n{df_}")
df_.info


cls_ = AnbimaDataIDALIQGeral(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA DATA INDEXES IDA LIQ GERAL: \n{df_}")
df_.info


cls_ = AnbimaDataIDKAPre1A(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF ANBIMA DATA INDEXES IDKA PRE1A: \n{df_}")
df_.info