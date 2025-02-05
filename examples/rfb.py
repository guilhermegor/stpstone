# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.registration.br.rfb.companies_public_data import CompaniesRFB
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.connections.netops.session import ReqSession


session = ReqSession(bl_new_proxy=False).session
df_ = CompaniesRFB(
    bl_new_proxy=False,
    bl_verify=False,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 5), 
    session=session,
    cls_db=None
).get_raw_data('companies')
print(df_)