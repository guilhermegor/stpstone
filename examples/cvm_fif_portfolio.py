"""CVM Liquid Funds Portfolio Composition (Lamina Carteira) from the FIF Lamina dataset."""

from stpstone.ingestion.countries.br.registries.cvm_fif_portfolio import CvmFIFPortfolio


cls_ = CvmFIFPortfolio(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF FIF Portfolio: \n{df_}")
df_.info()
