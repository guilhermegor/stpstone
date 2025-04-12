from stpstone.ingestion.countries.br.exchange.anbima_550_listing import Anbima550Listing
from stpstone.utils.cals.handling_dates import DatesBR


cls_ = Anbima550Listing(
    session=None,
    cls_db=None,
    dt_ref=DatesBR().sub_working_days(DatesBR().curr_date, 1),
)

df_ = cls_.source("550_listing", bl_fetch=True)
print(f"DF ANBIMA 550 LISTING: \n{df_}")
df_.info()
