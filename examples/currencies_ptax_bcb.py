# pypi.org libs
import os


# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.countries.br.macroeconomics.ptax_bcb import PTAXBCB
from stpstone.utils.calendars.calendar_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


session = YieldFreeProxy(bool_new_proxy=False).session

class_ptax = PTAXBCB(
    session=session,
    date_start=DatesBR().sub_working_days(DatesBR().curr_date(), 5),
    date_end=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
    cls_db=None,
    bool_debug=False
)
df_composition_ptax = class_ptax.composition_currency
print(f'DF PTAX BCB: \n{df_composition_ptax}')
print(df_composition_ptax.info())
