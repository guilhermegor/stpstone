"""CVM Liquid Funds Composition and Diversification of Assets from the FIF CDA dataset."""

from stpstone.ingestion.countries.br.registries.cvm_fif_cda import CvmFIFCDA


cls_ = CvmFIFCDA(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF CDA: \n{df_}")
df_.info()
