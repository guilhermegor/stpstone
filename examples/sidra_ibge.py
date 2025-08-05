from stpstone.ingestion.countries.br.macroeconomics.sidra_ibge import SidraIBGE


# cls_ = SidraIBGE()
# df_ = cls_.source("pools_releases_dates", bool_fetch=True)
# print(f"DF POOL RELEASES DATES: \n{df_}")
# df_.info()

# cls_ = SidraIBGE()
# df_ = cls_.source("sidra_modification_dates", bool_l_fetch=True)
# print(f"DF SIDRA MODIFICATION DATES: \n{df_}")
# df_.info()

cls_ = SidraIBGE()
df_ = cls_.source("sidra_variables", bool_l_fetch=True)
print(f"DF SIDRA VARIABLES: \n{df_}")
df_.info()