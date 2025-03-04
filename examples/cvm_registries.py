# pypi.org libs
import os
import sys

# local libs
sys.path.append(os.path.abspath(os.path.join(os.path.realpath(__file__), "..", "..")))
from stpstone.ingestion.countries.br.registries.cvm import CVMRegistries
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.utils.parsers.folders import DirFilesManagement

session = ReqSession(bl_new_proxy=False).session

cls_ = CVMRegistries(session=None, cls_db=None)

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

# df_ = cls_.source('incentivized_cos', bl_fetch=True, bl_debug=False)
# print(f'\n*** DF INVENTIVIZED COMPANIES COMPANIES - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('securities_consultants', bl_fetch=True, bl_debug=False)
# print(f'\n*** DF SECURITIES CONSULTANTS - CVM: \n{df_}')
# df_.info()
# df_.to_excel(
#     rf'{DirFilesManagement().find_project_root()}\data\cvm-securities-consultants_'
#     + rf'{DatesBR().curr_date.strftime('%Y%m%d')}_{DatesBR().curr_time.strftime('%H%M%S')}.xlsx',
#     index=False
# )

# df_ = cls_.source('crowdfunding', bl_fetch=True, bl_debug=False)
# print(f'\n*** DF CROWDFUNDING - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source("funds_registries", bl_fetch=True, bl_debug=False)
# print(f"\n*** DF FUNDS REGISTRIES - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("funds_daily_infos", bl_fetch=True, bl_debug=False)
# print(f"\n*** DF FUNDS DAILY INFOS - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("funds_fact_sheet", bl_fetch=True, bl_debug=False)
# print(f"\n*** DF FUNDS FACT SHEET - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("funds_monthly_profile", bl_fetch=True, bl_debug=False)
# print(f"\n*** DF FUNDS MONTHLY PROFILE - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("funds_abs", bl_fetch=True, bl_debug=False)
# print(f"\n*** DF FUNDS ABS - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("specially_constitued_investment_fund", bl_fetch=True, bl_debug=False)
# print(f"\n*** DF SPECIALLY CONSTITUED INVESTMENT FUND - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("reits_monthly_profile", bl_fetch=True, bl_debug=False)
# print(f"\n*** DF REITS MONTHLY PROFILE - CVM: \n{df_}")
# df_.info()

df_ = cls_.source("banks_registry", bl_fetch=True, bl_debug=False)
print(f"\n*** DF BANKS REGISTRY - CVM: \n{df_}")
df_.info()
