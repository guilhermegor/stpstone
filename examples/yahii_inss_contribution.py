"""Yahii INSS Contribution Brackets for Brazil"""

from stpstone.ingestion.countries.br.macroeconomics.yahii_inss_contribution import (
	YahiiINSSContribution,
)


cls_ = YahiiINSSContribution(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF YAHII INSS CONTRIBUTION: \n{df_}")
df_.info()
