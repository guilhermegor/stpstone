from stpstone.ingestion.countries.us.registries.slickcharts_indexes_components import (
    SlickChartsIndexesComponents,
)


cls_ = SlickChartsIndexesComponents()

df_ = cls_.source("sp500", bool_fetch=True)
print(f"DF SLICKCHARTS INDEXES COMPONENTS - S&P 500: \n{df_}")
df_.info()

df_ = cls_.source("nasdaq100", bool_l_fetch=True)
print(f"DF SLICKCHARTS INDEXES COMPONENTS - NASDAQ 100: \n{df_}")
df_.info()

df_ = cls_.source("dowjones", bool_l_fetch=True)
print(f"DF SLICKCHARTS INDEXES COMPONENTS - DOW JONES: \n{df_}")
df_.info()
