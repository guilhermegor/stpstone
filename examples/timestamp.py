from datetime import datetime, timezone

from stpstone.utils.calendars.calendar_abc import DatesBR


# convert to Unix timestamp
date_ = datetime(2024, 11, 25, 12, 58, 5, tzinfo=timezone.utc)
timestamp_ = DatesBR().datetime_to_unix_timestamp(date_)
print(date_, timestamp_)

date_ = DatesBR().sub_working_days(DatesBR().curr_date(), 5)
timestamp_ = DatesBR().datetime_to_unix_timestamp(date_)
print(date_, timestamp_)

date_ = DatesBR().sub_working_days(DatesBR().curr_date(), 0)
timestamp_ = DatesBR().datetime_to_unix_timestamp(date_)
print(date_, timestamp_)

# timestamp to datetime
timestamp_ = 1739827800
date_ = DatesBR().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, date_)

timestamp_ = 1739827800
date_ = DatesBR().unix_timestamp_to_date(timestamp_)
print(timestamp_, date_)

timestamp_ = 1740087000
date_ = DatesBR().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, date_)

timestamp_ = 1740173400
date_ = DatesBR().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, date_)
