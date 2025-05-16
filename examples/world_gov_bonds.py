import pandas as pd
from stpstone.ingestion.countries.ww.macroeconomics.world_gov_bonds import WorldGovBonds


pd.set_option("display.max_rows", None)

cls_ = WorldGovBonds()

df_ = cls_.source("sovereign_spreads", bl_fetch=True)
print(f"DF WORLD GOV BONDS - SOVEREIGN SPREADS: \n{df_}")
df_.info()

df_ = cls_.source("countries_ratings", bl_fetch=True)
print(f"DF WORLD GOV BONDS - COUNTRIES RATINGS: \n{df_}")
df_.info()

df_ = cls_.source("sovereign_cds", bl_fetch=True)
print(f"DF WORLD GOV BONDS - SOVEREIGN CDS: \n{df_}")
df_.info()