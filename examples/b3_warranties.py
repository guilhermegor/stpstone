"""B3 Warranties."""

from stpstone.ingestion.countries.br.exchange.b3_warranties import (
    B3WarrantiesBRSovereignBonds,
    B3WarrantiesStocksUnitsETFs,
)


cls_ = B3WarrantiesBRSovereignBonds(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 WARRANTIES BR SOVEREIGN BONDS: \n{df_}")
df_.info()


cls_ = B3WarrantiesStocksUnitsETFs(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run(bool_verify=False)
print(f"DF B3 WARRANTIES STOCKS UNITS ETFs: \n{df_}")
df_.info()