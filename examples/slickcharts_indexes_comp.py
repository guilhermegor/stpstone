from stpstone.ingestion.countries.us.registries.slickcharts_indexes_components import SlickChartsIndexesComponents

cls_ = SlickChartsIndexesComponents()

df_ = cls_.source("sp500", bl_fetch=True)
print(f"DF SLICKCHARTS INDEXES COMPONENTS - S&P 500: \n{df_}")
df_.info()

df_ = cls_.source("nasdaq100", bl_fetch=True)
print(f"DF SLICKCHARTS INDEXES COMPONENTS - NASDAQ 100: \n{df_}")
df_.info()

df_ = cls_.source("dowjones", bl_fetch=True)
print(f"DF SLICKCHARTS INDEXES COMPONENTS - DOW JONES: \n{df_}")
df_.info()
