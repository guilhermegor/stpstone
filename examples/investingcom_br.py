from stpstone.ingestion.countries.br.macroeconomics.investingcom_br import InvestingComBR

cls_ = InvestingComBR()
df_ = cls_.source("ipca_forecast", bl_fetch=True)
print(f"DF INVESTING COM - IPCA FORECAST: \n{df_}")
df_.info()