from stpstone.ingestion.countries.br.exchange.indexes_anbima import AnbimaIndexes


cls_ = AnbimaIndexes(
    session=None,
    cls_db=None
)

df_ = cls_.source("anbima_indexes_mkt_data", bl_fetch=True)
print(f"DF ANBIMA INDEXES MKT DATA: \n{df_}")
df_.info()

df_ = cls_.source("anbima_indexes_portf_composition", bl_fetch=True)
print(f"DF ANBIMA INDEXES PORTF COMPOSITION: \n{df_}")
df_.info()
