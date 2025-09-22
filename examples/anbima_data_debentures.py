from stpstone.ingestion.countries.br.registries.anbima_data_debentures import (
    AnbimaDataDebenturesAvailable,
)


cls_ = AnbimaDataDebenturesAvailable(
    int_pg_start=4,
    int_pg_end=10, 
    date_ref=None, 
    logger=None,
    cls_db=None, 
    int_default_timeout_miliseconds=30_000,
    int_wait_next_request_seconds=10,
)

df_ = cls_.run()
print(f"DF ANBIMA DATA DEBENTURES AVAILABLE: \n{df_}")
df_.info()