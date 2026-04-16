"""FRED US macroeconomic data series."""

from stpstone.ingestion.countries.us.macroeconomics.fred import FredUSMacro


cls_ = FredUSMacro(api_key="YOUR_API_KEY", date_ref=None, logger=None, cls_db=None)
df_ = cls_.run()
print(f"DF FRED: \n{df_}")
df_.info()
