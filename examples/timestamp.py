from datetime import datetime, timezone

from stpstone.utils.calendars.calendar_br import DatesBRAnbima


# convert to Unix timestamp
date_ = datetime(2024, 11, 25, 12, 58, 5, tzinfo=timezone.utc)
timestamp_ = DatesBRAnbima().datetime_to_unix_timestamp(date_)
print(date_, timestamp_)

date_ = DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 5)
timestamp_ = DatesBRAnbima().datetime_to_unix_timestamp(date_)
print(date_, timestamp_)

date_ = DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 0)
timestamp_ = DatesBRAnbima().datetime_to_unix_timestamp(date_)
print(date_, timestamp_)

# timestamp to datetime
timestamp_ = 1739827800
date_ = DatesBRAnbima().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, date_)

timestamp_ = 1739827800
date_ = DatesBRAnbima().unix_timestamp_to_date(timestamp_)
print(timestamp_, date_)

timestamp_ = 1740087000
date_ = DatesBRAnbima().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, date_)

timestamp_ = 1740173400
date_ = DatesBRAnbima().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, date_)
