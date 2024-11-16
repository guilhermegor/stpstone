### FUNÇÕES ÚTEIS PARA INTERACAO ENTRE DATAS ###

import locale
import time
import businesstimedelta
import pytz
import pandas as pd
from datetime import timedelta, date, datetime, time, timezone
from dateutil.relativedelta import relativedelta
from workalendar.core import MON, SAT, SUN
from more_itertools import unique_everseen
from pprint import pprint
from stpstone.cals.br_bzdays import BrazilBankCalendar
from stpstone.handling_data.str import StrHandler


class DatesBR(BrazilBankCalendar):
    '''
    DOCSTING: CALENDÁRIO DE DIAS ÚTEIS POR CIDADES NO BRASIL, CALENDÁRIO DE FERIADOS BANCÁRIOS E
    FUNÇÕES COM DATAS (ÚLTIMO DIA ÚTIL BANCÁRIO, PRÓXIMO DIA ÚTIL, É DIA ÚTIL?, CONVERSÃO DE DATAS
    DE STR PARA INT (VISANDO REALIZAR CÁLCULOS COM A DATA) E INTERVALO DE DATAS ÚTEIS ENTRE OS DOIS
    LIMITES (PASSADOS COMO ARGUMENTO)
    '''

    def build_date(self, year, month, day):
        '''
        DOCSTRING: BUILD DATETIME WITH YEAR, MONTH AND DAY INFO
        INPUTS: YEAR, MONTH, DAY
        OUTPUTS: DATETIME
        '''
        return date(year=year, month=month, day=day)

    def build_datetime(self, year, month, day, hour, minute, second):
        '''
        DOCSTRING: BUILD DATETIME WITH YEAR, MONTH AND DAY INFO
        INPUTS: YEAR, MONTH, DAY
        OUTPUTS: DATETIME
        '''
        return datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

    def date_to_datetime(self, date, bl_crop_time=True, bl_tinestamp=True):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        datetime_ = datetime.combine(date, datetime.min.time())
        if bl_tinestamp == True:
            datetime_ = datetime_.timestamp()
        if bl_crop_time == True:
            return int(datetime_)
        else:
            return datetime_

    def to_integer(self, dt_time):
        '''
        DOCSTRING: CONVERT DATETIME TO INTEGER
        INPUTS: DATETIME
        OUTPUTS: INTEGER
        '''
        return 10000 * dt_time.year + 100 * dt_time.month + dt_time.day

    def excel_float_to_date(self, float_excel_date):
        '''
        DOCSTRING: RETRIEVE DATE FROM EXCEL DATE FORMAT
        INPUTS: EXCEL DATE
        OUTPUTS: DATE
        '''
        return date.fromordinal(date(1900, 1, 1).toordinal() + float_excel_date - 2)

    def excel_float_to_datetime(self, float_excel_date):
        '''
        DOCSTRING: RETRIEVE DATETIME FROM EXCEL DATE FORMAT
        INPUTS: EXCEL DATE
        OUTPUTS: DATETIME
        '''
        return datetime.fromordinal(datetime(1900, 1, 1).toordinal() + float_excel_date - 2)

    def check_date_datetime_format(self, curr_date):
        '''
        DOCSTRING: CHECK WHETER OR NOT THE DATE FORMAT IS DATETIME
        INPUTS: DATE
        OUTPUTS: BOOLEAN
        '''
        return isinstance(curr_date, date)

    def str_date_to_datetime(self, data_str, format='DD/MM/YYYY'):
        '''
        DOCSTRING: CONVERSÃO DE DATA DE STRING PARA DATETIME.DATE, VISANDO REALIZAR CONTAS COM A MESMA
        INPUTS: DATA STRING NO format DD/MM/YYYY, YYYY-MM-DD, YYMMDD, DDMMYY OU DDMMYYYY
        OUTPUTS: DATA INT NO format YYYY-MM-DD
        '''
        if format == 'DD/MM/YYYY':
            return date(int(data_str[-4:]), int(data_str[3:5]), int(data_str[0:2]))
        elif format == 'YYYY-MM-DD':
            return date(int(data_str[0:4]), int(data_str[5:7]), int(data_str[-2:]))
        elif format == 'YYMMDD':
            return date(int('20' + data_str[0:2]), int(data_str[2:4]), int(data_str[-2:]))
        elif format == 'DDMMYY':
            return date(int('20' + data_str[-2:]), int(data_str[2:4]), int(data_str[0:2]))
        elif format == 'DDMMYYYY':
            return date(int(data_str[0:2]), int(data_str[2:4]), int(data_str[-4:]))
        elif format == 'DD/MM/YY':
            int_year = int(str(DatesBR().year_number(DatesBR().curr_date))[:2] + str(data_str[-2:]))
            return date(int_year, int(data_str[3:5]), int(data_str[0:2]))
        else:
            raise Exception('format não é válido, favor revisite o parâmetro')

    def datetime_to_string(self, curr_date, date_format='%d/%m/%Y'):
        '''
        DOCSTRING: DATETIME TO STRING IN DESIGNED FORMAT
        INPUTS: CURRENTE DATE IN DATETIME AND DATE FORMAT
        OUTPUTS: STRING
        '''
        return curr_date.strftime(date_format)

    def date_to_datetime(self, date, bl_crop_time=True, bl_tinestamp=True):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        datetime_ = datetime.combine(date, datetime.min.time())
        if bl_tinestamp == True:
            datetime_ = datetime_.timestamp()
        if bl_crop_time == True:
            return int(datetime_)
        else:
            return datetime_

    def list_range_number_working_days_between_dates(self, data_inic, data_fim, du_ant_ref_date=0,
                                                     format_datas_input=None):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # correção format datas
        if type(data_inic) == str:
            data_inic = DatesBR().str_dates_to_datetime(data_inic, format_datas_input)
        if type(data_fim) == str:
            data_fim = DatesBR().str_dates_to_datetime(data_fim, format_datas_input)
        # criando data de referência
        ref_date = self.sub_working_days(self.curr_date(), du_ant_ref_date)
        # definindo du_anteriores inf e sup, em relação à data de interesse
        du_inf = self.get_working_days_delta(data_inic, ref_date)
        du_sup = self.get_working_days_delta(data_fim, ref_date)
        # retornando range de du anteriores
        return range(du_sup, du_inf + 1)

    def list_working_days(self, data_inic, data_fim, format_datas_input=None,
                          format_datas_saida='%d/%m/%Y'):
        '''
        DOCSTRING: DIAS ÚTEIS ENTRE DUAS DATAS, CONSIDERANDO OS FERIADOS BANCÁRIOS NO BRASIL
        INPUTS: DATA DE INÍCIO, DATA FIM EM DATETIME.DATE, STR ('DD/MM/YYYY' OU 'YYYY-MM-DD')
            E format DA DATA
        OUTPUTS: LISTA DE DIAS ÚTEIS ENTRE DUAS DATAS EM DATETIME.DATE
        '''
        # correção format datas
        if str(type(data_inic)) == "<class 'str'>":
            data_inic = DatesBR().str_dates_to_datetime(data_inic, format_datas_input)
        if str(type(data_fim)) == "<class 'str'>":
            data_fim = DatesBR().str_dates_to_datetime(data_fim, format_datas_input)
        # lista de passagem
        list_du = list()
        # * super() just works in its own scope, however list comprehensions create its own scope
        # * so we have to append an empty list
        for x in range(int((data_fim - data_inic).days)):
            list_du.append(super().find_following_working_day(
                day=data_inic + timedelta(days=x)))
        if format_datas_saida != None:
            return[DatesBR().datetime_to_string(d, format_datas_saida) for d in
                   list(unique_everseen(list_du))]
        else:
            return list(unique_everseen(list_du))

    def list_calendar_days(self, data_inic, data_fim, format_data='DD/MM/YYYY'):
        '''
        DOCSTRING: DIAS CORRIDOS ENTRE DUAS DATAS
        INPUTS: DATA DE INÍCIO E DATA FIM EM DATETIME.DATE (INT)
        OUTPUTS: LISTA DE DIAS ÚTEIS ENTRE DUAS DATAS EM DATETIME.DATE
        '''
        # correção format datas
        if str(type(data_inic)) == "<class 'str'>":
            data_inic = DatesBR().str_dates_to_datetime(data_inic, format_data)
        if str(type(data_fim)) == "<class 'str'>":
            data_fim = DatesBR().str_dates_to_datetime(data_fim, format_data)
        # lista de passagem
        list_du = list()
        # appendando dias úteis
        # * super() just works in its own scope, however list comprehensions create its own scope
        # * so we have to append an empty list
        for x in range(int((data_fim - data_inic).days)):
            list_du.append(data_inic + timedelta(days=x))
        return list(unique_everseen(list_du))

    def list_years_within_dates(self, data_inic, data_fim):
        '''
        DOCSTRING: LISTA DE ANOS ENTRE DATAS
        INPUTS: DATA DE INÍCIO E DATA FIM EM DATETIME.DATE (INT)
        OUTPUTS: LISTA DE ANOS ENTRE DUAS DATAS
        '''
        # correção format datas
        if str(type(data_inic)) == "<class 'str'>":
            data_inic = DatesBR().str_dates_to_datetime(data_inic, 'DD/MM/YYYY')
        if str(type(data_fim)) == "<class 'str'>":
            data_fim = DatesBR().str_dates_to_datetime(data_fim, 'DD/MM/YYYY')
        # lista de passagem
        list_years = list()
        # appendando dias úteis
        # * super() just works in its own scope, however list comprehensions create its own scope
        # * so we have to append an empty list
        for x in range(int((data_fim - data_inic).days)):
            list_years.append((data_inic + timedelta(days=x)).year)
        return list(unique_everseen(list_years))

    @property
    def curr_date(self):
        '''
        DOCSTRING: RETORNA A DATA DE HOJE EM DATETIME.DATE
        INPUTS: -
        OUTPUTS: DATA DE HOJE EM DATETIME.DATE
        '''
        return date.today()

    @property
    def curr_time(self):
        '''
        DOCSTRING: RETORNA O HORÁRIO LOCAL NO MOMENTO DA EXECUÇÃO DA DEF
        INPUTS: -
        OUTPUTS: HORA DE AGORA EM DATETIME.DATE
        '''
        return datetime.now().time()

    def curr_date_time(self, bl_timestamp=False, bl_crop_time=False):
        '''
        DOCSTRING: RETORNA HORA E DATA LOCAL ATUAL
        INPUTS: -
        OUTPUTS: DATA E HORA DE AGORA NO format EM DATETIME.DATE
        '''
        # validating wheter the user wants to return the timestamp format or not
        if bl_timestamp == True:
            datetime_ = datetime.now().timestamp()
        else:
            datetime_ = datetime.now()
        # crop time if its user's will
        if bl_crop_time == True:
            return int(datetime_)
        else:
            return datetime_

    def testing_dates(self, data_inic, data_fim):
        '''
        DOCSTRING: TESTAR SE DATAS INSERIDAS RESPEIAM UM CRITÉRIO LÓGICO
        INPUTS: DATA DE INÍCIO E DE FIM
        OUTPUTS: RESPOSTA À SOLICITAÇÃO
        '''
        if type(data_inic) != datetime:
            data_inic = DatesBR().date_to_datetime(data_inic)
        if type(data_fim) != datetime:
            data_fim = DatesBR().date_to_datetime(data_fim)
        if int((data_fim - data_inic).days) >= 0:
            return True
        else:
            return True

    def year_number(self, date):
        '''
        DOCSTRING: YEAR NUMBER
        INPUTS: DATE (AS DATETIME)
        OUPUTS: INTEGER
        '''
        return int(date.strftime('%Y'))

    def day_number(self, date):
        '''
        DOCSTRING: MONTH NUMBER
        INPUTS: DATE (AS DATETIME)
        OUPUTS: INTEGER
        '''
        return int(date.strftime('%d'))

    def month_name(self, date, abbreviation=False, local_zone='pt-BR'):
        '''
        DOCSTRING: NAME OF THE MONTH IN THE LOCAL LANGUAGE
        INPUTS: DATE, ABBREVIATION AND LOCAL ZONE (BRAZIL AS STANDARD)
        OUTPUTS: STRING WITH THE NAME OF THE MONTH
        '''
        locale.setlocale(locale.LC_TIME, local_zone)
        if abbreviation == True:
            return date.strftime('%b')
        else:
            return date.strftime('%B')

    def date_begin_end_month(self, date_, last_month_year=12, format='YYYY-MM-DD'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        # converting input date to the correct format
        if type(date_) == str:
            self.str_dates_to_datetime(date_, format)
        # defining inputs for the dates range
        year = self.year_number(date_)
        month = self.month_number(date_)
        day = 1
        # building inferior and superior dates
        date_inf = self.find_working_day(self.build_date(year, month, day))
        if month < last_month_year:
            date_sup = self.sub_working_days(
                self.build_date(year, month + 1, day), 1)
        else:
            date_sup = self.sub_working_days(
                self.build_date(year + 1, 1, day), 1)
        # returning dates
        return date_inf, date_sup

    def month_number(self, date, bl_month_mm=False):
        '''
        DOCSTRING: MONTH NUMBER
        INPUTS: DATE (AS DATETIME)
        OUPUTS: INTEGER/STRING
        '''
        if bl_month_mm == False:
            return int(date.strftime('%m'))
        else:
            return date.strftime('%m')

    def week_name(self, date, abbreviation=False, local_zone='pt-BR'):
        '''
        DOCSTRING: NAME OF THE WEEKDAY IN THE LOCAL LANGUAGE
        INPUTS: DATE, ABBREVIATION AND LOCAL ZONE (BRAZIL AS STANDARD)
        OUTPUTS: STRING WITH THE NAME OF THE WEEK
        '''
        locale.setlocale(locale.LC_TIME, local_zone)
        if abbreviation == True:
            return date.strftime('%a')
        else:
            return date.strftime('%A')

    def week_number(self, date):
        '''
        DOCSTRING: NUMBER OF THE WEEKDAY
        INPUTS: DATE (AS DATETIME)
        OUTPUTS: INT
        '''
        return date.strftime('%w')

    def find_working_day(self, date_test):
        '''
        DOCSTRING: FIND NEXT WORKING DAY IF THE CURRENT DATE IS NOT A WORKING DAY
        INPUTS: DATE TO BE TESTED
        OUTPUTS: DATE
        '''
        return DatesBR().add_working_days(DatesBR().sub_working_days(date_test, 1), 1)

    def nth_weekday_month(self, date_inf, date_sup, weekday_number, nth_repetition,
                          date_format='DD/MM/YYYY', number_days_week=7):
        '''
        REFERENCES: https://stackoverflow.com/questions/57082058/get-every-nth-weekday-of-month-periodic-in-pandas
        DOCSTRING: GET NTH WORKDAY OF THE MONTH
        INPUTS:
        OUTPUTS: DATE
        '''
        list_working_days_within_dates = DatesBR().list_working_days(
            date_inf, date_sup, date_format)
        return [DatesBR().add_working_days(DatesBR().sub_working_days(d, 1), 1)
                for d in list_working_days_within_dates
                if (DatesBR().week_number(d) == weekday_number
                    and d.day >= (nth_repetition * number_days_week - number_days_week)
                    and d.day <= (nth_repetition * number_days_week))]

    def delta_calendar_days(self, inf_date, sup_date):
        '''
        DOCSTRING: COUNT CALENDAR DAYS, CONSIDERING NON-WORKING DATES
        INPUTS: INFERIOR AND SUOPERIOR DATES
        OUTPUTS: INT
        '''
        return (sup_date - inf_date).days

    def add_months(self, date_datetime, num_months):
        '''
        DOCSTRING: ADD MONTHS TO DATETIME
        INPUTS: DATE (DATETIME FORMAT) AND NUMBER OF MONTHS
        OUTPUTS: DATETIME
        '''
        return date_datetime + relativedelta(months=num_months)

    def add_calendar_days(self, original_date, days_to_add):
        '''
        DOCSTRING: ADD CALENDAR DAYS
        INPUTS: ORIGINAL DATE AND DAYS TO ADD
        OUTPUTS: DATETIME
        '''
        return original_date + timedelta(days=days_to_add)

    def delta_working_hours(self, inf_time_stamp, sup_time_stamp, start_time_office_hours=8,
                            end_time_office_hours=18, start_time_lunch_hours=0,
                            end_time_lunch_hours=0, working_days=[0, 1, 2, 3, 4]):
        '''
        REFERENCES: https://pypi.org/project/businesstimedelta/
        DOCSTRING: DELTA WORKING HOURS BETWEEN TWO TIMESTAMPS
        INPUTS: INFERIOR TIME STAMP AND SUPERIOR TIME STAMP, AS YYYY-MM-DD HH:MM:SS, START
            TIME AT OFFICE (8 AS STANDARD), END TIME AT OFFICE (18 AS STANDARD), START TIME
            FOR LUNCH (0 AS STANDARD), END TIME FOR LUNCH (0 AS STANDARD), WORKING DAYS (
                LIST FROM 0, MONDAY, TO 4, FRIDAY)
        OUTPUTS: TIME (HH:MM:SS), IN DATETIME.TIME FORMAT
        '''
        # timestamp convertation to datetime
        y_inf, mt_inf, d_inf = int(inf_time_stamp.split(' ')[0].split('-')[0]), \
            int(inf_time_stamp.split(' ')[0].split('-')[1]), \
            int(inf_time_stamp.split(' ')[0].split('-')[2])
        h_inf, m_inf, s_inf = int(inf_time_stamp.split(' ')[1].split(':')[0]), \
            int(inf_time_stamp.split(' ')[1].split(':')[1]), \
            int(inf_time_stamp.split(' ')[1].split(':')[2])
        y_sup, mt_sup, d_sup = int(sup_time_stamp.split(' ')[0].split('-')[0]), \
            int(sup_time_stamp.split(' ')[0].split('-')[1]), \
            int(sup_time_stamp.split(' ')[0].split('-')[2])
        h_sup, m_sup, s_sup = int(sup_time_stamp.split(' ')[1].split(':')[0]), \
            int(sup_time_stamp.split(' ')[1].split(':')[1]), \
            int(sup_time_stamp.split(' ')[1].split(':')[2])
        inf_time_stamp = datetime(y_inf, mt_inf, d_inf, h_inf, m_inf, s_inf)
        sup_time_stamp = datetime(y_sup, mt_sup, d_sup, h_sup, m_sup, s_sup)
        # dict of holidays
        dict_holidays_raw = dict()
        for y in range(inf_time_stamp.year, sup_time_stamp.year + 1):
            dict_holidays_raw[y] = DatesBR().holidays(y)
        dict_holidays_trt = dict()
        for k, v in dict_holidays_raw.items():
            for t in v:
                dict_holidays_trt[t[0]] = t[1]
        # office hours for working days
        workday = businesstimedelta.WorkDayRule(
            start_time=time(start_time_office_hours),
            end_time=time(end_time_office_hours),
            working_days=working_days)
        lunchbreak = businesstimedelta.LunchTimeRule(
            start_time=time(start_time_lunch_hours),
            end_time=time(end_time_lunch_hours),
            working_days=working_days)
        holidays = businesstimedelta.HolidayRule(dict_holidays_trt)
        businesshrs = businesstimedelta.Rules(
            [workday, lunchbreak, holidays])
        # output
        return businesshrs.difference(inf_time_stamp, sup_time_stamp).timedelta

    def list_last_days_of_years(self, list_years):
        '''
        DOCSTRING: LAST DAYS OF YEAR FOR A GIVEN LIST
        INPUTS: LIST OF YEARS
        OUTPUTS: LIST
        '''
        list_last_day_week_year = list()
        for y in list_years:
            last_day = date(y, 12, 31)
            if last_day.weekday() == SAT:
                list_last_day_week_year.append(last_day - timedelta(days=1))
            elif last_day.weekday() == SUN:
                list_last_day_week_year.append(last_day - timedelta(days=2))
            else:
                list_last_day_week_year.append(last_day)
        return list_last_day_week_year

    def add_holidays_not_considered_anbima(self, inf_date, sup_date, list_last_week_year_day,
                                           local_zone='pt-BR',
                                           list_holidays_not_considered=[
                                               '25/01'],
                                           list_dates_not_considered=[
                                               '05/03/2025', '18/02/2026'],
                                           list_non_bzdays_week=['sábado', 'domingo']):
        '''
        DOCSTRING: HOLIDAYS FOR SAO PAULO CALENDAR, FOR INSTANCE, THAT ARE CONSIDERED WORKING
            DAYS FOR ANBIMA OUGHT BE INCLUDED PRINCING-WISE
        INPUTS: INFERIOR DATE, SUPERIOR DATE, LIST OF LAST WEEK YEAR DAY,
            LOCAL ZONE (BRAZIL AS DEFAULT), LIST HOLIDAYS NOT CONSIDERED (AS STR DATES),
            LIST DATES NOT CONSIDERED (AS STR), LIST NON-BUSINESS DAYS
        OUTPUTS: NUMBER OF WORKING DAYS TO ADD
        '''
        locale.setlocale(locale.LC_TIME, local_zone)
        if str(type(inf_date)) == "<class 'str'>":
            inf_date = DatesBR().str_dates_to_datetime(inf_date, 'DD/MM/YYYY')
        if str(type(sup_date)) == "<class 'str'>":
            sup_date = DatesBR().str_dates_to_datetime(sup_date, 'DD/MM/YYYY')
        return len([d for d in DatesBR().list_calendar_days(inf_date, sup_date) if
                    (d.strftime('%d/%m') in list_holidays_not_considered and
                     not DatesBR().week_name(d) in list_non_bzdays_week or
                     d in list_last_week_year_day or d.strftime('%d/%m/%Y') in
                     list_dates_not_considered)])

    def timestamp_float_to_datetime(self, timestamp, format_timestamp='%Y-%m-%d %H:%M:%S',
                                    bl_format=True):
        '''
        DOCSTRING: TIMESTAMP FLOAT TO DATETIME
        INPUTS: TIMESTAMP AND ITS OUTPUT FORMAT AS DATETIME
        OUTPUTS: DATETIME
        '''
        if bl_format == True:
            return datetime.fromtimestamp(timestamp).strftime(format_timestamp)
        else:
            return datetime.fromtimestamp(timestamp)

    def timestamp_separator_string_to_datetime(self, str_timestamp, substring_datetime='T',
                                               format='YYYY-MM-DD'):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS
        '''
        return self.str_dates_to_datetime(
            StrHandler().get_string_until_substr(str_timestamp, substring_datetime), format)

    def timestamp_to_datetime(self, timestamp, bl_return_from_utc=False):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        if bl_return_from_utc == True:
            return pd.to_datetime(timestamp, unit='s', utc=True).tz_convert('America/Sao_Paulo')
        else:
            return pd.to_datetime(timestamp, unit='s', utc=True).strftime('%Y%m%d')

    def datetime_timestamp(self, date, bl_date_to_datetime=False):
        '''
        DOCSTRING: DATETIME TO TIMESTAMP
        INPUTS: DATE
        OUTPUTS: TIMESTAMP
        '''
        # converting date to datetime
        if bl_date_to_datetime == True:
            date = datetime.combine(date, datetime.min.time())
        return datetime.timestamp(date)

    def current_timestamp_string(self, format='%Y%m%d_%H%M%S'):
        '''
        DOCSTRING: CURRENT TIMESTAMP IN STRING FORMAT
        INPUTS: FORMAT (DEFAULT VALUE)
        OUTPUTS: STRING
        '''
        return DatesBR().curr_date_time().strftime(format)
    
    @property
    def utc_log_ts(self):
        '''
        DOCSTRING: UTC LOG TIMESTAMP
        INPUTS: -
        OUTPUTS: TIMESTAMP
        '''
        return datetime.now(timezone.utc)

    def utc_from_timestamp(self, date_):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        return datetime.utcfromtimestamp(date_)
    
    def utc_from_dt(self, date_):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        date_ = datetime.combine(date_, datetime.min.time())
        return pytz.utc.localize(date_)
    
    def month_year_string(self, str_dt, format_input='%b/%Y', format_output='%Y-%m', bl_dtbr=True):
        '''
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        '''
        if bl_dtbr == True:
            month_mapping = {
                "JAN": "01",
                "FEB": "02",
                "MAR": "03",
                "APR": "04",
                "MAY": "05",
                "JUN": "06",
                "JUL": "07",
                "AUG": "08",
                "SEP": "09",
                "OCT": "10",
                "NOV": "11",
                "DEC": "12",
                "JAN": "01",
                "FEV": "02",
                "MAR": "03",
                "ABR": "04",
                "MAI": "05",
                "JUN": "06",
                "JUL": "07",
                "AGO": "08",
                "SET": "09",
                "OUT": "10",
                "NOV": "11",
                "DEZ": "12",
            }
            month_abbr, year = str_dt.split('/')
            month = month_mapping[month_abbr.upper()]
            return f'{year}-{month}'
        else:
            return datetime.strptime(str_dt, format_input).strftime(format_output)


# print(DatesBR().delta_working_hours(
#     '2020-09-04 08:35:17', '2020-09-08 12:42:33'))
# # output
# 14:07:16
# print(DatesBR().delta_working_hours(
#     '2020-09-04 17:15:00', '2020-09-08 08:33:17'))
# # output
# 01:18:17
# print(DatesBR().delta_working_hours(
#     '2020-09-04 17:15:00', '2020-09-04 18:43:33'))
# # output
# 00: 45: 00
# print(DatesBR().delta_working_hours(
#     '2020-09-04 17:15:33', '2020-09-04 17:18:12'))
# # output
# 00:02:39
# print(DatesBR().delta_working_hours(
#     '2020-08-11 05:24:32', '2020-09-08 17:18:12'))
# print(DatesBR().add_holidays_not_considered_anbima('10/08/2020', '05/01/2021'))
# # output
# 2
# # output
# 220
# 3
# print(DatesBR().week_name(DatesBR().str_dates_to_datetime(inf_date, 'DD/MM/YYYY')))
# inf_date = '02/01/2026'
# sup_date = '01/07/2026'
# list_years = DatesBR().list_years_within_dates(inf_date, sup_date)
# list_last_week_year_day = DatesBR().list_last_days_of_years(list_years)
# print(DatesBR().list_working_days(inf_date, sup_date))
# print(DatesBR().get_working_days_delta(DatesBR().str_dates_to_datetime(inf_date, 'DD/MM/YYYY'),
#                                        DatesBR().str_dates_to_datetime(sup_date, 'DD/MM/YYYY')))
# print(DatesBR().add_holidays_not_considered_anbima(
#     inf_date, sup_date, list_last_week_year_day))
# print(DatesBR().is_holiday(
#     DatesBR().str_dates_to_datetime('10/02/2027', 'DD/MM/YYYY')))
