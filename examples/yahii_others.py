import pandas as pd

from stpstone.ingestion.countries.br.macroeconomics.yahii_others import YahiiOthersBR
from stpstone.utils.calendars.calendar_abc import DatesBR


pd.set_option("display.max_rows", None)

cls_ = YahiiOthersBR(
    session=None,
    date_ref=DatesBR().sub_working_days(DatesBR().curr_date(), 300),
    cls_db=None
)

# df_ = cls_.source("min_wage", bool_fetch=True)
# print(f"DF YAHII MIN WAGE: \n{df_}")
# df_.info()

df_ = cls_.source("inss_contribution", bool_fetch=True)
print(f"DF YAHII INSS CONTRIBUTION: \n{df_}")
df_.info()

# df_ = cls_.source("daily_usdbrl", bool_fetch=True)
# print(f"DF YAHII DAILY USD/BRL: \n{df_}")
# df_.info()

# df_ = cls_.source("daily_eurbrl", bool_fetch=True)
# print(f"DF YAHII DAILY EUR/BRL: \n{df_}")
# df_.info()
