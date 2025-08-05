from stpstone.ingestion.countries.us.registries.etfdb_vettafi import EtfDBVettaFi


cls_ = EtfDBVettaFi(bool_headless=True, bool_incognito=True, int_wait_load_seconds=60)

df_ = cls_.source("reits", bool_l_fetch=True)
print(f"DF ETFDB VETTAFI: \n{df_}")
df_.info()
