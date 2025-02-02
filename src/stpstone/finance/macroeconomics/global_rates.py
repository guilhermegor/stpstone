### COLLECT WORLD GOVERNMENT BONDS DATA ###

import pandas as pd

from stpstone.cals.handling_dates import DatesBR
from stpstone.handling_data.dicts import HandlingDicts
from stpstone.handling_data.html import HtmlHndler
from stpstone.handling_data.numbers import NumHandler
from stpstone.handling_data.str import StrHandler
from stpstone.loggs.db_logs import DBLogs
from stpstone.settings._global_slots import YAML_GLB_RT


class GlobalRates:
    def generic_req(self, url, bl_verify=False):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        # setting variables
        list_outlooks = list()
        # request html
        bs_html = HtmlHndler().html_bs_parser(url, bl_verify)
        # table
        bs_table = bs_html.find('table')
        # headers
        list_th = [
            YAML_GLB_RT['general']['col_rate']
            if len(th.get_text().replace('\n', '').replace('/', '_').strip())
            == 0
            else th.get_text()
            .replace('\n', '')
            .replace('/', '_')
            .strip()
            .upper()
            for th in bs_table.find_all('th')
        ]
        # data
        list_td = [
            #   turning into percentage values that are numbers
            float(td.get_text().replace('\n', '').replace('%', '').strip())
            / 100.0
            if NumHandler().is_numeric(
                td.get_text().replace('\n', '').replace('%', '').strip()
            )
            #   return value otherwise
            else td.get_text().replace('\n', '').replace('%', '').strip()
            for td in bs_table.find_all('td')
            if len(
                td.get_text()
                .replace('\n', '')
                .replace('%', '')
                .replace('-', '0')
                .strip()
            )
            > 0
        ]
        # checking wheter the table has direction info
        try:
            #   outlooks
            for div in bs_table.find_all(
                'div', class_='table-normal text-end'
            ):
                try:
                    str_outlook = StrHandler().replace_all(
                        div.find('i')['class'][1],
                        YAML_GLB_RT['central_banks_rates'][
                            'rate_outlook_desc'
                        ],
                    )
                    list_outlooks.append(str_outlook)
                except (AttributeError, TypeError):
                    continue
            #   input direction in the data dictionary
            int_col_dir = list_th.index(
                YAML_GLB_RT['central_banks_rates']['col_direction']
            )
            int_rng_upper = len(list_td) // (len(list_th) - 1)
            for i in range(int_rng_upper):
                list_td.insert(3 + (int_col_dir + 3) * i, list_outlooks[i])
        except ValueError:
            pass
        # creating list of dictionaries
        list_ser = HandlingDicts().pair_headers_with_data(list_th, list_td)
        # turning into dataframe
        df_ = pd.DataFrame(list_ser)
        # adding logging
        df_ = DBLogs().audit_log(
            df_,
            DatesBR().utc_from_dt(DatesBR().curr_date),
            DatesBR().utc_log_ts,
        )
        # return dataframe
        return df_

    @property
    def libor(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['libor_rates']['url'],
            YAML_GLB_RT['libor_rates']['bl_verify'],
        )

    @property
    def euribor(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['euribor_rates']['url'],
            YAML_GLB_RT['euribor_rates']['bl_verify'],
        )

    @property
    def central_banks(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['central_banks_rates']['url'],
            YAML_GLB_RT['central_banks_rates']['bl_verify'],
        )

    @property
    def ester(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['ester']['url'], YAML_GLB_RT['ester']['bl_verify']
        )

    @property
    def sonia(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['sonia']['url'], YAML_GLB_RT['sonia']['bl_verify']
        )

    @property
    def sofr(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['sofr']['url'], YAML_GLB_RT['sofr']['bl_verify']
        )

    @property
    def usa_cpi(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['usa_cpi']['url'], YAML_GLB_RT['usa_cpi']['bl_verify']
        )

    @property
    def uk_cpi(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['uk_cpi']['url'], YAML_GLB_RT['uk_cpi']['bl_verify']
        )

    @property
    def can_cpi(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['can_cpi']['url'], YAML_GLB_RT['can_cpi']['bl_verify']
        )

    @property
    def eur_cpi(self):
        """
        DOCSTRING:
        INPUTS:
        OUTPUTS:
        """
        return self.generic_req(
            YAML_GLB_RT['eur_cpi']['url'], YAML_GLB_RT['eur_cpi']['bl_verify']
        )
