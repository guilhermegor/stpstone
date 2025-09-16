"""Example of ingestion of B3 Financial Indicators data."""

from stpstone.ingestion.countries.br.macroeconomics.b3_financial_indicators import (
    B3FinancialIndicators,
)


cls_ = B3FinancialIndicators(
    date_ref=None,
    logger=None, 
    cls_db=None
)

df_ = cls_.run()
print(f"DF B3 FINANCIAL INDICATORS: \n{df_}")
df_.info