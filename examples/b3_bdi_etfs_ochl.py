"""B3 BDI ETFs IOPV (Indicative Optimized Portfolio Value) ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_etfs_ochl import B3BdiEtfsOchl


cls_ = B3BdiEtfsOchl(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI ETFs IOPV: \n{df_}")
df_.info()
