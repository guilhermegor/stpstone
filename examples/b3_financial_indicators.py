from stpstone.ingestion.countries.br.macroeconomics.b3_financial_indicators import B3FinancialIndicators


cls_ = B3FinancialIndicators()
df_ = cls_.source("resource", bl_fetch=True)
print(f"DF B3 FINANCIAL INDICATORS: \n{df_}")
df_.info()