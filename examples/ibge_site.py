"""Example of IBGE disclosure economic indicators."""

from stpstone.ingestion.countries.br.macroeconomics.ibge_site import (
	IBGEDisclosureEconomicIndicators,
)


cls_ = IBGEDisclosureEconomicIndicators(logger=None, cls_db=None)

df_ = cls_.run()
print(f"DF IBGE DISCLOSURE ECONOMIC INDICATORS: \n{df_}")
df_.info
