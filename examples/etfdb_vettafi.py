from stpstone.ingestion.countries.us.registries.etfdb_vettafi import EtfDBVettaFi


cls_ = EtfDBVettaFi(bl_headless=True, bl_incognito=True, int_wait_load=60)

df_ = cls_.source("reits", bl_fetch=True)
print(f"DF ETFDB VETTAFI: \n{df_}")
df_.info()
