from stpstone.ingestion.countries.br.macroeconomics.yahii_rates import YahiiRatesBRMacro
from stpstone.utils.calendars.calendar_abc import DatesBR


cls_ = YahiiRatesBRMacro(
    session=None,
    date_ref=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
    cls_db=None
)

df_ = cls_.source("pmi_rf_rates", bool_fetch=True)
print(f"DF YAHII: \n{df_}")
df_.info()
df_.to_excel(
    f'data/yahii-pmi-rf-rates_{DatesBR().curr_date().strftime("%Y%m%d")}_{DatesBR().curr_time().strftime("%H%M%S")}.xlsx',
    index=False)
