from stpstone.ingestion.countries.br.exchange.anbima_550_listing import Anbima550Listing
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


cls_ = Anbima550Listing(
    session=None,
    cls_db=None,
    date_ref=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
)

df_ = cls_.source("550_listing", bool_fetch=True)
print(f"DF ANBIMA 550 LISTING: \n{df_}")
df_.info()
