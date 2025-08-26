from stpstone.ingestion.countries.br.exchange.anbima_550_listing import Anbima550Listing
from stpstone.utils.calendars.calendar_abc import DatesBR


cls_ = Anbima550Listing(
    session=None,
    cls_db=None,
    date_ref=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
)

df_ = cls_.source("550_listing", bool_fetch=True)
print(f"DF ANBIMA 550 LISTING: \n{df_}")
df_.info()
