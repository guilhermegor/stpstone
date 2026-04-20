"""B3 BDI economic indicators."""

from stpstone.ingestion.countries.br.macroeconomics.b3_bdi_macroeconomics_economic_indicators import (  # noqa: E501
    B3BdiMacroeconomicsEconomicIndicators,
)


cls_ = B3BdiMacroeconomicsEconomicIndicators(
    date_ref=None,
    logger=None,
    cls_db=None,
)

df_ = cls_.run()
print(f"DF B3 BDI MACROECONOMICS ECONOMIC INDICATORS: \n{df_}")
df_.info()
