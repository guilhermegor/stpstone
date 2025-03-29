# pypi.org libs
import os
# local libs
os.path.abspath(os.path.join(os.path.realpath(__file__), '..'))
from stpstone.ingestion.registries.br.taxation.irsbr_records import IRSBR
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.sessions.proxy_scrape import ProxyScrape


session = ProxyScrape(bl_new_proxy=True).session
df_ = IRSBR(
    session=session,
    bl_create_session=False,
    bl_new_proxy=False,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 5),
    cls_db=None
).source('companies')
print(df_)
