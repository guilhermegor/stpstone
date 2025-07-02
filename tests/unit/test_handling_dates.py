from datetime import date, datetime, time, timedelta
import locale
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from stpstone.utils.cals.handling_dates import DatesBR


class TestDatesBR(unittest.TestCase):
    def setUp(self):
        self.dates_br = DatesBR()
        locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

    # existing tests (keep these)
    # ... (all your existing test methods)

    # new tests for uncovered functionality
    def test_excel_float_to_datetime(self):
        result = self.dates_br.excel_float_to_datetime(45000)  # excel date for 2023-03-15
        self.assertEqual(result, datetime(2023, 3, 15))

    def test_check_is_date(self):
        self.assertTrue(self.dates_br.check_is_date(self.dates_br.build_date(2023, 5, 15)))
        self.assertTrue(self.dates_br.check_is_date(datetime(2023, 5, 15)))
        self.assertTrue(self.dates_br.check_is_date("2023-05-15"))

    def test_str_date_to_datetime_other_formats(self):
        # test all format variations
        self.assertEqual(self.dates_br.str_date_to_datetime("150523", "DDMMYY"),
                         self.dates_br.build_date(2023, 5, 15))
        self.assertEqual(self.dates_br.str_date_to_datetime("15052023", "DDMMYYYY"),
                         self.dates_br.build_date(2023, 5, 15))
        self.assertEqual(self.dates_br.str_date_to_datetime("20230515", "YYYYMMDD"),
                         self.dates_br.build_date(2023, 5, 15))
        self.assertEqual(self.dates_br.str_date_to_datetime("05-15-2023", "MM-DD-YYYY"),
                         self.dates_br.build_date(2023, 5, 15))
        self.assertEqual(self.dates_br.str_date_to_datetime("230515", "YYMMDD"),
                         self.dates_br.build_date(2023, 5, 15))
        self.assertEqual(self.dates_br.str_date_to_datetime("15/05/23", "DD/MM/YY"),
                         self.dates_br.build_date(2023, 5, 15))
        self.assertEqual(self.dates_br.str_date_to_datetime("15.05.23", "DD.MM.YY"),
                         self.dates_br.build_date(2023, 5, 15))

    def test_list_cds(self):
        start = self.dates_br.build_date(2023, 5, 1)
        end = self.dates_br.build_date(2023, 5, 3)
        result = self.dates_br.list_cds(start, end)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], datetime(2023, 5, 1, 0, 0))

    def test_list_years(self):
        start = self.dates_br.build_date(2023, 1, 1)
        end = self.dates_br.build_date(2025, 1, 30)
        result = self.dates_br.list_years(start, end)
        self.assertEqual(result, [2023, 2024, 2025])

    def test_curr_date_and_time(self):
        with patch('stpstone.utils.cals.handling_dates.date') as mock_date:
            mock_date.today.return_value = self.dates_br.build_date(2023, 5, 15)
            self.assertEqual(self.dates_br.curr_date(), self.dates_br.build_date(2023, 5, 15))

        with patch('stpstone.utils.cals.handling_dates.datetime') as mock_datetime:
            mock_datetime.now.return_value.time.return_value = time(12, 30)
            self.assertEqual(self.dates_br.curr_time(), time(12, 30))

    def test_testing_dates(self):
        self.assertTrue(self.dates_br.testing_dates(self.dates_br.build_date(2023, 5, 1),
                                                    self.dates_br.build_date(2023, 5, 15)))
        self.assertFalse(self.dates_br.testing_dates(self.dates_br.build_date(2023, 5, 15),
                                                     self.dates_br.build_date(2023, 5, 1)))

    def test_year_day_month_numbers(self):
        test_date = self.dates_br.build_date(2023, 5, 15)
        self.assertEqual(self.dates_br.year_number(test_date), 2023)
        self.assertEqual(self.dates_br.day_number(test_date), 15)
        self.assertEqual(self.dates_br.month_number(test_date), 5)
        self.assertEqual(self.dates_br.month_number(test_date, True), "05")

    def test_month_and_week_names(self):
        test_date = self.dates_br.build_date(2023, 5, 15)  # Monday
        self.assertEqual(self.dates_br.month_name(test_date), "maio")
        self.assertEqual(self.dates_br.month_name(test_date, True), "mai")
        self.assertEqual(self.dates_br.week_name(test_date), "segunda")
        self.assertEqual(self.dates_br.week_name(test_date, True), "seg")
        self.assertEqual(self.dates_br.week_number(test_date), "1")  # Monday is 1 in this format

    def test_nth_weekday_month(self):
        start = self.dates_br.build_date(2023, 5, 1)
        end = self.dates_br.build_date(2023, 5, 31)
        # get all Mondays in May 2023
        mondays = self.dates_br.nth_weekday_month(start, end, 1, 1)
        self.assertEqual(len(mondays), 4)

    def test_delta_calendar_days(self):
        start = self.dates_br.build_date(2023, 5, 1)
        end = self.dates_br.build_date(2023, 5, 15)
        self.assertEqual(self.dates_br.delta_calendar_days(start, end), 14)

    def test_add_months_edge_cases(self):
        # test year rollover
        self.assertEqual(self.dates_br.add_months(
            self.dates_br.build_date(2023, 11, 15), 2).date(),
                         self.dates_br.build_date(2024, 1, 15))
        # test short month
        self.assertEqual(self.dates_br.add_months(
            self.dates_br.build_date(2023, 1, 31), 1).date(),
                         self.dates_br.build_date(2023, 2, 28))

    def test_delta_working_hours(self):
        start = "2023-05-15 09:00:00"  # monday
        end = "2023-05-15 17:00:00"    # monday
        # 8 working hours (9-17) minus 1 hour lunch
        self.assertEqual(self.dates_br.delta_working_hours(start, end),
                         timedelta(seconds=8 * 3600))

    def test_last_wd_years(self):
        # last working day of 2023 is Friday, Dec 29 (30th and 31st are weekend)
        last_days = self.dates_br.last_wd_years([2023])
        self.assertEqual(last_days[0], self.dates_br.build_date(2023, 12, 29))

    def test_unix_timestamp_conversions(self):
        test_dt = datetime(2023, 5, 15, 12, 0, 0)
        timestamp = self.dates_br.datetime_to_unix_timestamp(test_dt)

        # test roundtrip
        self.assertEqual(
            self.dates_br.unix_timestamp_to_datetime(timestamp).replace(tzinfo=None).date(),
            test_dt.date()
        )
        self.assertEqual(
            self.dates_br.unix_timestamp_to_date(timestamp),
            test_dt.date()
        )

    def test_iso_to_unix_timestamp(self):
        iso_str = "2023-05-15T12:00:00-03:00"  # brazil time (UTC-3)
        timestamp = self.dates_br.iso_to_unix_timestamp(iso_str)
        self.assertEqual(timestamp, 1684162800)  # expected Unix timestamp

    def test_timestamp_to_datetime(self):
        timestamp = 1684162800  # 2023-05-15 15:00:00 UTC
        # test with timezone conversion
        sao_paulo_dt = self.dates_br.timestamp_to_datetime(timestamp, "America/Sao_Paulo")
        self.assertEqual(sao_paulo_dt.hour, 12)  # UTC-3

    def test_current_timestamp_string(self):
        with patch('stpstone.utils.cals.handling_dates.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2023, 5, 15, 12, 30, 45)
            self.assertEqual(
                self.dates_br.current_timestamp_string(),
                "20230515_123045"
            )

    def test_utc_conversions(self):
        local_dt = datetime(2023, 5, 15, 12, 0, 0)
        utc_dt = self.dates_br.utc_from_dt(local_dt)
        self.assertEqual(utc_dt.tzinfo, ZoneInfo("UTC"))

    def test_month_year_string(self):
        # test Brazilian format conversion
        self.assertEqual(
            self.dates_br.month_year_string("MAY/2023"),
            "2023-05"
        )

    # edge case tests
    def test_str_date_to_datetime_invalid_format(self):
        with self.assertRaises(Exception): # noqa: B017 - do not assert blind exception
            self.dates_br.str_date_to_datetime("2023-05-15", "INVALID_FORMAT")

    def test_add_months_with_negative(self):
        self.assertEqual(
            self.dates_br.add_months(self.dates_br.build_date(2023, 3, 15), -2).date(),
            self.dates_br.build_date(2023, 1, 15)
        )

    def test_timestamp_to_date_without_substring(self):
        timestamp = "1684162800000"  # 2023-05-15 in milliseconds
        result = self.dates_br.timestamp_to_date(timestamp, None).date()
        self.assertEqual(result, datetime(2023, 5, 15, 15, 0).date())

    def test_dates_inf_sup_month_december(self):
        # test December to January transition
        dec_date = date(2023, 12, 15)
        start, end = self.dates_br.dates_inf_sup_month(dec_date)
        self.assertEqual(end, date(2023, 12, 29))  # last working day of December 2023

if __name__ == '__main__':
    unittest.main()
