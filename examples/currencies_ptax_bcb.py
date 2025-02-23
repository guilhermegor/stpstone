# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.macroeconomics.br.currencies.ptax_bcb import PTAXBCB
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


session = ReqSession(bl_new_proxy=False).session

class_ptax = PTAXBCB(
    session=session,
    dt_inf=DatesBR().sub_working_days(DatesBR().curr_date, 5), 
    dt_sup=DatesBR().sub_working_days(DatesBR().curr_date, 1), 
    cls_db=None, 
    bl_debug=False
)
df_composition_ptax = class_ptax.composition_currency
print(f'DF PTAX BCB: \n{df_composition_ptax}')
print(df_composition_ptax.info())