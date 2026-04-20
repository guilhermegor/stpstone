"""B3 BDI indexes OCHL (Open, Closing, High, Low) ingestion."""

from stpstone.ingestion.countries.br.exchange.b3_bdi_indexes_ochl import B3BdiIndexesOchl


cls_ = B3BdiIndexesOchl(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI Indexes OCHL: \n{df_}")
df_.info()
