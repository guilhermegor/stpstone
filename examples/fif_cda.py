"""CVM Liquid Funds Composition and Diversification of Assets from the FIF CDA dataset."""

from stpstone.ingestion.countries.br.registries.fif_cda import FIFCDA


cls_ = FIFCDA(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF CDA: \n{df_}")
df_.info()
