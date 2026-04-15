"""CVM Securities Distribution Offers from the CVM OFERTA dataset."""

from stpstone.ingestion.countries.br.registries.cvm_data_distribution_offers import (
    CVMDataDistributionOffers,
)


cls_ = CVMDataDistributionOffers(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF CVM Data Distribution Offers: \n{df_}")
df_.info()
