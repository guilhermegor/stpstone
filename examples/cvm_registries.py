# pypi.org libs
import os
import sys
# local libs
sys.path.append(os.path.abspath(os.path.join(os.path.realpath(__file__), '..', '..')))
from stpstone.ingestion.countries.br.registries.cvm import CVMRegistries
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession


session = ReqSession(bl_new_proxy=False).session

cls_ = CVMRegistries(
    session=None,
    cls_db=None
)

# df_ = cls_.source('independent_fin_advs', bl_fetch=True, bl_debug=False)
# print(f'\n*** DF INDEPENDENT FINANCIAL ADVISORS - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('fiduciary_agents', bl_fetch=True, bl_debug=False)
# print(f'\n*** DF FIDUCIARY AGENTS - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('pubicly_traded_cos', bl_fetch=True, bl_debug=False)
# print(f'\n*** DF PUBICLY TRADED COMPANIES - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('foreign_cos', bl_fetch=True, bl_debug=False)
# print(f'\n*** DF FOREIGN COMPANIES COMPANIES - CVM: \n{df_}')
# df_.info()

df_ = cls_.source('incentivized_cos', bl_fetch=True, bl_debug=False)
print(f'\n*** DF INVENTIVIZED COMPANIES COMPANIES - CVM: \n{df_}')
df_.info()