### USA WORKING DAYS ###

from datetime import datetime, timedelta
from workalendar.usa import UnitedStates


class WorkCalendar:

    def __init__(self) -> None:
        self.calendar = UnitedStates()

    def add_working_days(self, dt_bgn:datetime, int_num_days:int) -> datetime:
        current_date = dt_bgn
        working_days_added = 0
        while working_days_added < int_num_days:
            current_date += timedelta(days=1)
            if current_date.weekday() < 5 and not self.calendar.is_holiday(current_date):
                working_days_added += 1
        return current_date

    def is_holiday(self, date:datetime) -> bool:
        return self.calendar.is_holiday(date)

    def is_weekend(self, date:datetime) -> bool:
        return date.weekday() >= 5

    def diff_working_days(self, dt_bgn:datetime, dt_end:datetime) -> int:
        current_date = dt_bgn
        working_days = 0
        while current_date <= dt_end:
            if current_date.weekday() < 5 and not self.calendar.is_holiday(current_date):
                working_days += 1
            current_date += timedelta(days=1)
        return working_days


if __name__ == '__main__':

    cls_usabzdays = WorkCalendar()
    # current date
    dt_bgn = datetime(2025, 1, 1)
    # add 10 working days to the start date
    new_date = cls_usabzdays.add_working_days(dt_bgn, 10)
    print(f"Date after adding 10 working days: {new_date}")
    # check if a specific date is a holiday
    check_date = datetime(2025, 1, 20)  # Martin Luther King Jr. Day (USA)
    print(f"Is {check_date} a holiday? {cls_usabzdays.is_holiday(check_date)}")
    # check if a specific date is a weekend
    weekdt_sup = datetime(2025, 1, 11)
    print(f"Is {weekdt_sup} a weekend? {cls_usabzdays.is_weekend(weekdt_sup)}")
