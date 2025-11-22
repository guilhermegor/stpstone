"""Example of usage of AnbimaFundsRedemptionProbabilityMatrix class."""

from stpstone.ingestion.countries.br.registries.anbima_funds_withdraws_probability import (
    AnbimaFundsRedemptionProbabilityMatrix,
)


cls_ = AnbimaFundsRedemptionProbabilityMatrix(
    date_ref=None,
    logger=None, 
    cls_db=None, 
)

df_ = cls_.run()
print(f"DF ANBIMA REDEMPTION PROBABILITY MATRIX: \n{df_}")
df_.info()