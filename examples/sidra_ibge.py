from stpstone.ingestion.countries.br.macroeconomics.sidra_ibge import SidraIBGE


# cls_ = SidraIBGE()
# df_ = cls_.source("pools_releases_dates", bl_fetch=True)
# print(f"DF POOL RELEASES DATES: \n{df_}")
# df_.info()

cls_ = SidraIBGE()
df_ = cls_.source("sidra_modification_dates", bl_fetch=True)
print(f"DF SIDRA MODIFICATION DATES: \n{df_}")
df_.info()