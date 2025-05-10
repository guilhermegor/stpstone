from stpstone.ingestion.countries.br.macroeconomics.yahii import YahiiBRMacro
from stpstone.utils.cals.handling_dates import DatesBR

cls_ = YahiiBRMacro(
    session=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1),
    cls_db=None
)

df_ = cls_.source("pmi_rf_rates", bl_fetch=True)
print(f"DF YAHII: \n{df_}")
df_.info()
df_.to_excel(
    f'data/yahii-pmi-rf-rates_{DatesBR().curr_date.strftime("%Y%m%d")}_{DatesBR().curr_time.strftime("%H%M%S")}.xlsx',
    index=False)