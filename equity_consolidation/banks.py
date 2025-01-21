### INTEGRATION TO BANK APIS ###

import pandas as pd
from pynubank import Nubank, MockHttpClient
from stpstone.handling_data.json import JsonFiles
from stpstone.handling_data.pd import DealingPd
from stpstone.cals.handling_dates import DatesBR


class NubankIntegration:

    def authentification(self, individual_register_number, password, complete_path_cert_p12=None,
                         auth_method='qr_code', mock=True, uuid_mock='123'):
        '''
        REFERENCES: https://github.com/andreroggeri/pynubank/blob/master/examples/login-qrcode.md,
            https://github.com/andreroggeri/pynubank/blob/master/examples/login-certificate.md,
            https://github.com/andreroggeri/pynubank
        DOCSTRING: LOGIN WITH QR CODE
        INPUTS: INDIVIDUAL REGISTER NUMBER (CPF), PASSWORD, COMPLETE PATH CERTIFICATION P12 (
            LOGGING WITH CERTIFICATION TO COMMUNICATE WITH NUBANK SERVER, NONE AS DEFAULT),
            AUTHENTICATION METHOD, MOCK (ENTERING MODE, AS A BOOLEAN), UUID_MOCK (123 AS DEFAULT)
        OUTPUTS: NUBANK OBJECT
        '''
        # wheter or not log through mock authentication
        if mock == True:
            # nubank object
            nu = Nubank(MockHttpClient())
            # authenticate credentials
            nu.authenticate_with_qr_code(
                individual_register_number, password, uuid_mock)
            # return access to nubank objects, granted through authentification
            return nu
        elif mock == False:
            # nubank object
            nu = Nubank()
            # validating method of logging
            if auth_method == 'qr_code':
                # after this command will be displayed a QRCode in a console, please scan through
                #   the app to grante entrance; this is available in the menu: NU > Perfil > Acesso
                #   pelo site, in the brazillian portugues translation
                uuid, qr_code = nu.get_qr_code()
                qr_code.print_ascii(invert=True)
                input('After scanning, please hit any key to continue...')
                # authenticate credentials
                nu.authenticate_with_qr_code(
                    individual_register_number, password, uuid)
                # return access to nubank objects, granted through authentification
                return nu
            elif auth_method == 'certification':
                nu.authenticate_with_cert(
                    individual_register_number, password, complete_path_cert_p12)
                # return access to nubank objects, granted through authentification
                return nu
            else:
                raise Exception(
                    'Please consider the qr code or certification method to login')
        else:
            raise Exception('Authetication mock variable ouught be a boolean')

    def credit_card_bill(self, nu, bl_return_pd=True, list_ser=list(), float_den_amount=100.0,
                         col_id='ID', col_description='DESCRIPTION', col_category='CATEGORY',
                         col_subcategory='SUBCATEGORY', col_amount='AMOUNT',
                         col_amount_wo_iof='AMOUNT_WITHOUT_IOF', col_time='TIME',
                         col_source='SOURCE', col_title='TITLE', col_account='ACCOUNT',
                         col_status='STATUS', col_href_prod_global='HREF_PROD_GLOBAL',
                         bl_tokenized='BL_TOKENIZED', col_href_transaction='HREF_TRANSACTION',
                         col_lat='LAT', col_lon='LON'):
        '''
        REFERENCES: https://github.com/andreroggeri/pynubank
        DOCSTRING: NUBANK CREDIT CARD BILL
        INPUTS: NU OBJECT, RETURNED THROUGH AUTHENTIFICATION
        OUTPUTS: JSON (DICT WITH CARD STATEMENTS, BILLS AND BILLS DETAILS AS KEYS)
        '''
        # import dictionary with card statements
        list_dicts_card_statements = nu.get_card_statements()
        # return pandas dataframe, if it is user's will
        if bl_return_pd == True:
            #   looping through card statements aiming to build a pandas dataframe
            for dict_ in list_dicts_card_statements:
                #   dealing with eventually missing data
                if 'subcategory' in dict_['details']:
                    subcategory = dict_['details']['subcategory'].upper()
                else:
                    subcategory = 'N/A'
                if 'source' in dict_:
                    source = dict_['source'].upper()
                else:
                    source = 'N/A'
                if 'account' in dict_:
                    account = dict_['account'].upper()
                else:
                    account = 'N/A'
                if 'status' in dict_:
                    status = dict_['details']['status'].upper()
                else:
                    status = 'N/A'
                if 'tokenized' in dict_:
                    tokenized = str(dict_['tokenized']).upper()
                else:
                    tokenized = 'N/A'
                if 'href' in dict_:
                    href_transaction = dict_['href']
                else:
                    href_transaction = 'N/A'
                if 'amount_without_iof' in dict_:
                    amount_wo_iof = dict_['amount_without_iof']
                else:
                    amount_wo_iof = dict_['amount']
                if 'lat' in dict_['details']:
                    lat = dict_['details']['lat']
                else:
                    lat = 99999999
                if 'lon' in dict_['details']:
                    lon = dict_['details']['lon']
                else:
                    lon = 99999999
                #   serialized data to pandas dataframe
                list_ser.append({
                    col_id: dict_['id'].upper(),
                    col_description: dict_['description'].upper(),
                    col_category: dict_['category'].upper(),
                    col_subcategory: subcategory,
                    col_amount: dict_['amount'] / float_den_amount,
                    col_amount_wo_iof: amount_wo_iof / float_den_amount,
                    col_time: dict_['time'],
                    col_source: source,
                    col_title: dict_['title'].upper(),
                    col_account: account,
                    col_status: status,
                    col_href_prod_global: dict_['_links']['self']['href'],
                    bl_tokenized: tokenized,
                    col_href_transaction: href_transaction,
                    col_lat: lat,
                    col_lon: lon
                })
            #   pandas dataframe
            df_card_statements = pd.DataFrame(list_ser)
            #   changing column types
            df_card_statements = df_card_statements.astype({
                col_id: str,
                col_description: str,
                col_category: str,
                col_subcategory: str,
                col_amount: float,
                col_amount_wo_iof: float,
                col_time: str,
                col_source: str,
                col_title: str,
                col_account: str,
                col_status: str,
                col_href_prod_global: str,
                bl_tokenized: str,
                col_href_transaction: str,
                col_lat: float,
                col_lon: float
            })
            #   datetime colum types
            df_card_statements[col_time] = pd.to_datetime(
                df_card_statements[col_time], errors='ignore')
            #   sort values according to date and category
            df_card_statements.sort_values([col_time, col_category], ascending=[True, True],
                                           inplace=True)
            #   return pandas dataframe
            return df_card_statements
        # if raise an error in bills, return just card statements
        try:
            # list of dicts with all credit card bills
            bills = nu.get_bills()
            # get credit card bill details
            bills_details = [nu.get_bill_details(
                bills[i]) for i in range(1, len(bills))]
            # send nubank credit card message
            jsonify_message = {
                'card_statements': list_dicts_card_statements,
                'bills': bills,
                'bills_details': bills_details
            }
            return JsonFiles().send_json(jsonify_message)
        except:
            return list_dicts_card_statements

    def current_account(self, nu):
        '''
        REFERENCES: https://github.com/andreroggeri/pynubank
        DOCSTRING: NUBANK CURRENT ACCOUNT DETAILS
        INPUTS: NU OBJECT, RETURNED THROUGH AUTHENTIFICATION
        OUTPUTS: JSON (DICT WITH CARD STATEMENTS, BILLS AND BILLS DETAILS AS KEYS)
        '''
        jsonify_message = {
            'account_statements': nu.get_account_statements(),
            'current_account': nu.get_account_balance()
        }
        return JsonFiles().send_json(jsonify_message)
