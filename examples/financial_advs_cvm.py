# pypi.org libs
import os
import sys
# local libs
sys.path.append(os.path.abspath(os.path.join(os.path.realpath(__file__), '..', '..')))
from stpstone.ingestion.countries.br.registries.financial_advs_cvm import IndpFinAdvsCVM 
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


session = ReqSession(bl_new_proxy=False).session

class_ = IndpFinAdvsCVM(
    session=None,
    cls_db=None
)
df_ = class_.source('individuals_legal_entities', bl_fetch=True, bl_debug=False)
print(f'DF FINANCIAL ADVISORS - CVM: \n{df_}')
df_.info()