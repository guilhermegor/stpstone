from stpstone.ingestion.countries.br.macroeconomics.yahii_others import YahiiOthersBR
from stpstone.utils.cals.handling_dates import DatesBR

cls_ = YahiiOthersBR(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    cls_db=None
)

df_ = cls_.source("min_wage", bl_fetch=True)
print(f"DF YAHII MIN WAGE: \n{df_}")
df_.info()