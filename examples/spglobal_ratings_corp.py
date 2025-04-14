import pandas as pd
from stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal import RatingsCorpSPGlobal


list_ser = list()

str_bearer = RatingsCorpSPGlobal(bearer=None).get_bearer
print(f"Bearer String: {str_bearer}")

for i in range(1, 56):
    cls_ = RatingsCorpSPGlobal(pg_number=i, bearer=str_bearer)
    df_ = cls_.source("ratings_corp", bl_fetch=True)
    list_ser.extend(df_.to_dict(orient="records"))
df_ratings = pd.DataFrame(list_ser)
print(f"DF RATINGS CORP S&P GLOBAL: \n{df_ratings}")
df_ratings.info()
