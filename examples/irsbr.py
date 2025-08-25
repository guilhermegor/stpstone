# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), ".."))
from stpstone.ingestion.countries.br.taxation.irsbr_records import IRSBR
from stpstone.utils.cals.cal_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


df_ = IRSBR(
    session=None,
    date_ref=DatesBR().sub_working_days(DatesBR().curr_date(), 5),
    cls_db=None,
).source("companies")
print(df_)
