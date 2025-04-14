from stpstone.ingestion.countries.ww.registries.ratings_corp_spglobal import \
    RatingsCorpSPGlobalProduct


cls_ = RatingsCorpSPGlobalProduct()
df_ratings = cls_.get_corp_ratings
print(f"DF RATINGS CORP S&P GLOBAL: \n{df_ratings}")
df_ratings.info()
