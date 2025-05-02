from stpstone.ingestion.countries.br.macroeconomics.anbima_pmi_forecasts import AnbimaForecasts

cls_ = AnbimaForecasts(
    session=None,
    cls_db=None
)

df_ = cls_.source("mp1", bl_fetch=True)
print(f"DF ANBIMA FORECASTS MP1: \n{df_}")
df_.info()

df_ = cls_.source("mp2", bl_fetch=True)
print(f"DF ANBIMA FORECASTS MP2: \n{df_}")
df_.info()

df_ = cls_.source("ltm", bl_fetch=True)
print(f"DF ANBIMA FORECASTS LTM: \n{df_}")
df_.info()

