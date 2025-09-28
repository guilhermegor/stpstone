"""Example of anbima data debentures."""

from stpstone.ingestion.countries.br.registries.anbima_data_debentures import (
    AnbimaDataDebenturesAvailable,
)


cls_ = AnbimaDataDebenturesAvailable(
    date_ref=None, 
    logger=None, 
    cls_db=None, 
    start_page=1, 
    end_page=5,
)

df_ = cls_.run()
print(f"DF ANBIMA DEBENTURES: \n{df_}")
df_.info()