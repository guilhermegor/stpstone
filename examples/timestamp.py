from datetime import datetime, timezone
from stpstone.utils.cals.handling_dates import DatesBR


# convert to Unix timestamp
dt_ = datetime(2024, 11, 25, 12, 58, 5, tzinfo=timezone.utc)
timestamp_ = DatesBR().datetime_to_timestamp(dt_)
print(dt_, timestamp_)

dt_ = DatesBR().sub_working_days(DatesBR().curr_date, 5)
timestamp_ = DatesBR().datetime_to_timestamp(dt_)
print(dt_, timestamp_)

dt_ = DatesBR().sub_working_days(DatesBR().curr_date, 0)
timestamp_ = DatesBR().datetime_to_timestamp(dt_)
print(dt_, timestamp_)

# timestamp to datetime
timestamp_ = 1739827800
dt_ = DatesBR().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, dt_)

timestamp_ = 1739827800
dt_ = DatesBR().unix_timestamp_to_date(timestamp_)
print(timestamp_, dt_)

timestamp_ = 1740087000
dt_ = DatesBR().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, dt_)

timestamp_ = 1740173400
dt_ = DatesBR().unix_timestamp_to_datetime(timestamp_)
print(timestamp_, dt_)