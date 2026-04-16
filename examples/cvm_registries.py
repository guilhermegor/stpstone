# pypi.org libs
import os
import sys


# local libs
sys.path.append(os.path.abspath(os.path.join(os.path.realpath(__file__), "..", "..")))
from stpstone.ingestion.countries.br.registries.cvm import CVMRegistries
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy
from stpstone.utils.parsers.folders import DirFilesManagement


session = YieldFreeProxy(bool_new_proxy=False).session

cls_ = CVMRegistries(session=None, cls_db=None)

# df_ = cls_.source('independent_fin_advs', bool_fetch=True)
# print(f'\n*** DF INDEPENDENT FINANCIAL ADVISORS - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('fiduciary_agents', bool_fetch=True)
# print(f'\n*** DF FIDUCIARY AGENTS - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('pubicly_traded_cos', bool_fetch=True)
# print(f'\n*** DF PUBICLY TRADED COMPANIES - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('foreign_cos', bool_fetch=True)
# print(f'\n*** DF FOREIGN COMPANIES COMPANIES - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('incentivized_cos', bool_fetch=True)
# print(f'\n*** DF INVENTIVIZED COMPANIES COMPANIES - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source('securities_consultants', bool_fetch=True)
# print(f'\n*** DF SECURITIES CONSULTANTS - CVM: \n{df_}')
# df_.info()
# df_.to_excel(
#     rf'{DirFilesManagement().find_project_root()}\data\cvm-securities-consultants_'
#     + rf'{DatesBRAnbima().curr_date().strftime('%Y%m%d')}_{DatesBRAnbima().curr_time().strftime('%H%M%S')}.xlsx',
#     index=False
# )

# df_ = cls_.source('crowdfunding', bool_fetch=True)
# print(f'\n*** DF CROWDFUNDING - CVM: \n{df_}')
# df_.info()

# df_ = cls_.source("funds_registries", bool_fetch=True)
# print(f"\n*** DF FUNDS REGISTRIES - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("funds_daily_infos", bool_fetch=True)
# print(f"\n*** DF FUNDS DAILY INFOS - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("funds_fact_sheet", bool_fetch=True)
# print(f"\n*** DF FUNDS FACT SHEET - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("funds_monthly_profile", bool_fetch=True)
# print(f"\n*** DF FUNDS MONTHLY PROFILE - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("monthly_infos_fidcs", bool_fetch=True)
# print(f"\n*** DF FUNDS ABS - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("specially_constitued_investment_fund", bool_fetch=True)
# print(f"\n*** DF SPECIALLY CONSTITUED INVESTMENT FUND - CVM: \n{df_}")
# df_.info()

# df_ = cls_.source("reits_monthly_profile", bool_fetch=True)
# print(f"\n*** DF REITS MONTHLY PROFILE - CVM: \n{df_}")
# df_.info()

df_ = cls_.source("banks_registry", bool_fetch=True)
print(f"\n*** DF BANKS REGISTRY - CVM: \n{df_}")
df_.info()
