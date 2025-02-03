### COLLECT WORLD GOVERNMENT BONDS DATA

import pandas as pd
from datetime import datetime, timezone
from stpstone.settings._global_slots import YAML_WGBD
from stpstone.handling_data.html import HtmlHndler
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.handling_data.str import StrHandler
from stpstone.loggs.db_logs import DBLogs
from stpstone.cals.handling_dates import DatesBR


class WGBD:

    def parse_utc_dt_str(self, str_dt):
        """
        DOCSTRING: PARSER UTC DATETIME TO UTC FORMAT
        INTPUTS: STRING DATETIME
        OUTPUTS: DATETIME
        """
        # parse the date and time string without the timezone info
        dt_obj = datetime.strptime(str_dt[:-6], '%d %b %Y %H:%M')
        # manually set the UTC timezone
        dt_utc = dt_obj.replace(tzinfo=timezone.utc)
        # return datetime
        return dt_utc

    @property
    def bonds_10y_rates_rating(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_ser = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_WGBD['10y_rr']['url'], 
            bl_verify=YAML_WGBD['10y_rr']['bl_verify']
        )
        # getting last update date
        str_last_update = StrHandler().get_string_after_substr(
            bs_html.find('article').find(
                'div', class_=YAML_WGBD['10y_rr']['class_div_last_update']
            ).get_text(), 
            YAML_WGBD['10y_rr']['data_last_update']
        ).strip()
        # looping within tables
        for bs_table in bs_html.find_all('table'):
            #   collecting raw headers
            list_th_1 = [
                el.get_text()\
                    .replace('\n ', '')\
                    .replace('\n', '')\
                    .strip()\
                    .replace(' ', '_')\
                    .replace('-', '')\
                    .replace('▴', '')\
                    .replace('&', '_AND_')\
                    .upper()
                for el in bs_table.find('thead').find_all('tr')[0].find_all('div')
            ]
            list_th_2 = [
                el.get_text()\
                    .replace('\n ', '')\
                    .replace('\n', '')\
                    .strip()\
                    .replace(' ', '_')\
                    .replace('-', '')\
                    .replace('▴', '')\
                    .replace('&', '_AND_')\
                    .upper()
                for el in bs_table.find('thead').find_all('tr')[1].find_all('th')
            ]
            #   building consolidated headers' list
            list_th = [
                list_th_2[1], 
                list_th_1[0] + '_' + list_th_2[2],
                list_th_1[1] + '_' + list_th_2[3],
                list_th_1[2] + '_' + list_th_2[5],
                list_th_1[3] + '_' + list_th_2[6],
                list_th_1[3] + '_' + list_th_2[7],
                list_th_1[3] + '_' + list_th_2[8]
            ]
            #   looping within rows in tbody
            for bs_tr in bs_table.find('tbody').find_all('tr'):
                #   get all td data
                list_td = [
                    el.get_text()\
                        .replace('\n', '')\
                        .replace(' (*)', '')\
                        .replace('\t', '')\
                        .strip()
                    for el in bs_tr.find_all('td')
                    if (len(el.get_text()) > 1) & (el.get_text() != '')
                ]
                list_td = [x for x in list_td if len(x) > 0]
                #   case when len of tds is less than ths, then check the s&p rating, if is not a 
                #       string add an error tag
                if len(list_th) > len(list_td):
                    if StrHandler().bl_has_numbers(list_td[1]) == True:
                        list_td.insert(1, YAML_WGBD['10y_rr']['data_error'])
                    else:
                        raise Exception('Error - data missing in tbody unable to replace, please '
                                        + 'validate the code')
                #   creating list of dictionaries
                list_ser = HandlingDicts().pair_headers_with_data(
                    list_th, 
                    list_td
                )
        # creating dataframe
        df_b10_rr = pd.DataFrame(list_ser)
        # remove duplicates
        df_b10_rr.drop_duplicates(inplace=True)
        # adding logging
        df_b10_rr = DBLogs().audit_log(
            df_b10_rr, YAML_WGBD['10y_rr']['url'], 
            self.parse_utc_dt_str(str_last_update)
        )
        # returning dataframe
        return df_b10_rr
    
    @property
    def inv_yc_economies(self):
        """
        DOCSTRING: GET INVERTED YIELD CURVE ECONOMIES
        INPUTS: -
        OUTPUTS: TUPLE OF DATAFRAMES
        """
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_WGBD['inv_yc_ec']['url'], 
            bl_verify=YAML_WGBD['inv_yc_ec']['bl_verify']
        )
        # getting last update date
        str_last_update = StrHandler().get_string_after_substr(
                bs_html.find('article').find(
                    'p', class_='-f14').get_text(), 
                YAML_WGBD['inv_yc_ec']['data_last_update']
            ).strip()
        # looping within tables
        for i_tb, bs_table in enumerate(bs_html.find_all('table')):
            #   dealing with first table
            if i_tb == 0:
                #   reseting variables
                list_ser = list()
                #   looping within rows in tbody
                for bs_tr in bs_table.find('tbody').find_all('tr'):
                    #   collecting countries, rate and level of invertion
                    list_ = [
                        el.get_text()\
                            .replace('\n', '')\
                            .strip()
                        for el in bs_tr.find_all('td')
                    ]
                    #   catching current rate
                    str_rating = list_[0]
                    #   consolidating level of invertion
                    for str_lvl_inv, list_ in [
                        (YAML_WGBD['inv_yc_ec']['data_tot_inv'], 
                         [x.strip() for x in list_[1].split(YAML_WGBD['inv_yc_ec'][
                             'split_countries_lvl_inv'])]), 
                        (YAML_WGBD['inv_yc_ec']['data_prt_inv'], 
                         [x.strip() for x in list_[1].split(YAML_WGBD['inv_yc_ec'][
                             'split_countries_lvl_inv'])]), 
                        (YAML_WGBD['inv_yc_ec']['data_min_inv'], 
                         [x.strip() for x in list_[1].split(YAML_WGBD['inv_yc_ec'][
                             'split_countries_lvl_inv'])]), 
                    ]:
                        list_ser.extend([
                            {
                                YAML_WGBD['inv_yc_ec']['col_rating']: str_rating,
                                YAML_WGBD['inv_yc_ec']['col_country']: x,
                                YAML_WGBD['inv_yc_ec']['col_lvl_inv']: str_lvl_inv
                            } for x in list_
                        ])
                #   import list serialized to dataframe
                df_inv_yc_ec = pd.DataFrame(list_ser)
                # remove duplicates
                df_inv_yc_ec.drop_duplicates(inplace=True)
            #   dealing with first table
            elif i_tb == 1:
                #   reseting variables
                list_ser = list()
                #   collecting countries and yield spreads headers
                list_th = [
                    el.get_text()\
                        .replace('\n', '')\
                        .replace('Long vs Short Term', '')\
                        .replace('Mid vs Short Term', '')\
                        .replace('Short Term', '')\
                        .replace(' ', '_')\
                        .strip()\
                        .upper()
                    for el in bs_table.find('thead').find_all('th')
                    if len(el.get_text()) > 0
                ]
                #   collecting countries and yield spreads data
                list_td = [
                    #   collecting bps and transforming to float
                    float(
                        el.get_text()\
                            .replace('\n', '')\
                            .replace(YAML_WGBD['inv_yc_ec']['data_bp'], '')\
                            .strip()
                    ) * 1E-4
                    if StrHandler().match_string_like(
                        el.get_text()\
                            .replace('\n', '')\
                            .strip(), 
                        '* bp'
                    ) == True
                    #   collecting other data, such as country name
                    else el.get_text()\
                            .replace('\n', '')\
                            .strip()
                    for el in bs_table.find('tbody').find_all('td')
                    if len(el.get_text()\
                            .replace('\n', '')\
                            .strip()
                    ) > 0
                ]
                #   creating list of dictionaries
                list_ser = HandlingDicts().pair_headers_with_data(
                    list_th, 
                    list_td
                )
                #   import list serialized to dadtaframe
                df_inv_yc_spreads = pd.DataFrame(list_ser)
                #   remove duplicates
                df_inv_yc_spreads.drop_duplicates(inplace=True)
            else:
                raise Exception('Table not identified, of inverted yield curve countries, please '
                                + 'revisit the code')
        #   adding logging columns
        for df_ in [
            df_inv_yc_ec,
            df_inv_yc_spreads
        ]:
            #   adding logging
            df_ = DBLogs().audit_log(
                df_, YAML_WGBD['inv_yc_ec']['url'], 
                self.parse_utc_dt_str(str_last_update)
            )
        #   returning dataframes
        return df_inv_yc_ec, df_inv_yc_spreads
    
    @property
    def yield_spreads_nations(self):
        """
        DOCSTRING: GET YIELD SPREADS AGAINST GERMANY, USA, CHINA AND AUSTRALIA
        INPUTS: -
        OUTPUTS: DATAFRAME
        """
        # setting variables
        list_td = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_WGBD['yield_spreads_nations']['url'], 
            bl_verify=YAML_WGBD['yield_spreads_nations']['bl_verify']
        )
        # getting last update date
        str_last_update = StrHandler().get_string_after_substr(
            [
                el.get_text() for el in bs_html.find('article').find_all('p')
                if StrHandler().match_string_like(
                    el.get_text(),
                    YAML_WGBD['yield_spreads_nations']['data_last_update_like']
                )
            ][0], 
            YAML_WGBD['yield_spreads_nations']['data_last_update']
        ).strip()
        # getting headers
        list_th1 = [
            el.get_text()\
                .replace(' ', '_')\
                .strip()\
                .upper()
            for el in bs_html.find('table').find('thead').find_all('tr')[0].find_all('th')
            if len(el.get_text()) > 0
        ]
        list_th2 = [
            el.get_text()\
                .replace('▴', '')\
                .replace('\n', '')\
                .strip()\
                .upper()\
                .replace(' ', '_')
            for el in bs_html.find('table').find('thead').find_all('tr')[1].find_all('th')
            if len(el.get_text()) > 0
        ]
        list_th = [list_th2[0], list_th2[1]]
        list_th.extend([
            list_th1[0] + '_' + x for x in list_th2[2:]
        ])
        # getting data
        for el in bs_html.find('table').find('tbody').find_all('td'):
            #   ignoring missing data
            if len(
                el.get_text()\
                    .replace('\n', '')\
                    .strip()
            ) == 0: continue
            #   collecting bps data
            elif StrHandler().match_string_like(
                    el.get_text()\
                        .replace('\n', '')\
                        .strip(), 
                    '* bp'
                ) == True:
                list_td.append(float(
                    el.get_text()\
                        .replace('\n', '')\
                        .replace(YAML_WGBD['yield_spreads_nations']['data_bp'], '')\
                        .strip()
                ) * 1E-4)
            #   collecting 10y data
            elif StrHandler().match_string_like(
                    el.get_text()\
                        .replace('\n', '')\
                        .strip(), 
                    '*%'
            ) == True:
                list_td.append(float(
                    el.get_text()\
                        .replace('\n', '')\
                        .replace(YAML_WGBD['yield_spreads_nations']['data_pct'], '')\
                        .strip()
                ) * 1E-2)
            #   collecting other data, such as country name
            else:
                list_td.append(
                    el.get_text()\
                        .replace('\n', '')\
                        .strip()
                )
        # creating list of dictionaries
        list_ser = HandlingDicts().pair_headers_with_data(
            list_th, 
            list_td
        )
        # import list serialized to dadtaframe
        df_spreads_nations = pd.DataFrame(list_ser)
        # remove duplicates
        df_spreads_nations.drop_duplicates(inplace=True)
        # adding logging
        df_spreads_nations = DBLogs().audit_log(
            df_spreads_nations, YAML_WGBD['yield_spreads_nations']['url'], 
            self.parse_utc_dt_str(str_last_update)
        )
        # returning dataframe
        return df_spreads_nations
    
    @property
    def ratings(self):
        """
        DOCSTRING: GET RATINGS FROM THE MAIN AGENCIES
        INPUTS: -
        OUTPUTS: DATAFRAME
        """
        # setting variables
        list_td = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_WGBD['yield_spreads_nations']['url'], 
            bl_verify=YAML_WGBD['yield_spreads_nations']['bl_verify']
        )
        # getting last update date
        str_last_update = StrHandler().get_string_after_substr(
            [
                el.get_text() for el in bs_html.find('article').find_all('p')
                if StrHandler().match_string_like(
                    el.get_text(),
                    YAML_WGBD['yield_spreads_nations']['data_last_update_like']
                )
            ][0], 
            YAML_WGBD['yield_spreads_nations']['data_last_update']
        ).strip()
        # request html
        bs_html = HtmlHndler().html_bs_parser(
            YAML_WGBD['credit_rating_nations']['url'], 
            bl_verify=YAML_WGBD['credit_rating_nations']['bl_verify']
        )
        # getting headers
        list_th1 = [
            el.get_text()\
                .replace(' ', '_')\
                .replace('▴', '')\
                .replace('&', '_AND_')\
                .replace("'", '')\
                .strip()\
                .upper()
            for el in bs_html.find('table').find('thead').find_all('th')
            if len(el.get_text()) > 0
        ]
        list_th = [list_th1[0]]
        for i in range(1, len(list_th1)):
            list_th.append(list_th1[i])
            list_th.append(list_th1[i] + '_OUTLOOK')
        # getting data
        for el in bs_html.find('table').find('tbody').find_all('td'):
            #   check whether info is empty, in this case continue
            if len(el.get_text().strip()) == 0: 
                bl_country = True
                continue
            #   appending info regarding country and current rating
            list_td.append(el.get_text()\
                .strip())
            #   getting info regarding outlook
            try:
                list_td.append(
                    el.find('i')['title']\
                        .replace(' ', '_')\
                        .upper()
                )
            except (AttributeError, TypeError):
                if bl_country == False:
                    list_td.append(
                        'NEUTRAL_OUTLOOK'
                    )
                else:
                    bl_country = False
        # creating list of dictionaries
        list_ser = HandlingDicts().pair_headers_with_data(
            list_th, 
            list_td
        )
        # import list serialized to dadtaframe
        df_ratings = pd.DataFrame(list_ser)
        # remove duplicates
        df_ratings.drop_duplicates(inplace=True)
        # adding logging
        df_ratings = DBLogs().audit_log(
            df_ratings, YAML_WGBD['credit_rating_nations']['url'], 
            self.parse_utc_dt_str(str_last_update)
        )
        # returning dataframe
        return df_ratings