"""Yahii Minimum Wage History for Brazil"""

from stpstone.ingestion.countries.br.macroeconomics.yahii_min_wage import YahiiMinWage


cls_ = YahiiMinWage(
	date_ref=None,
	logger=None,
	cls_db=None,
)

df_ = cls_.run()
print(f"DF YAHII MINIMUM WAGE: \n{df_}")
df_.info()
