from getpass import getuser
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.countries.br.exchange.indexes_anbima_data import AnbimaDataIndexes


cls_ = AnbimaDataIndexes(
    session=None,
    cls_db=None
)

df_ = cls_.source("ima_geral", bl_fetch=True)
print(f"DF ANBIMA DATA IMA GERAL: \n{df_}")
df_.info()
df_.to_csv("data/anbima-ima-geral_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
), index=False)

df_ = cls_.source("ida_geral", bl_fetch=True)
print(f"DF ANBIMA DATA IDA GERAL: \n{df_}")
df_.info()
df_.to_csv("data/anbima-ida-geral_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
), index=False)

df_ = cls_.source("ida_liq_geral", bl_fetch=True)
print(f"DF ANBIMA DATA IDA LIQ GERAL: \n{df_}")
df_.info()
df_.to_csv("data/anbima-ida-liq-geral_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
), index=False)

df_ = cls_.source("idka_pre_1a", bl_fetch=True)
print(f"DF ANBIMA DATA IDKA PRE 1A: \n{df_}")
df_.info()
df_.to_csv("data/anbima-idka-pre-1a_{}_{}_{}.csv".format(
    getuser(),
    DatesBR().curr_date.strftime('%Y%m%d'),
    DatesBR().curr_time.strftime('%H%M%S')
), index=False)
