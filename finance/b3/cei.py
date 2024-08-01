### INFOS FROM CEI ENVIRONMENT OF INVESTOR'S TRADES IN BRAZILLIAN B3 MARKET ###

import asyncio
from bolsa import B3AsyncBackend
from pprint import pprint
from stpstone.cals.handling_dates import DatesBR


class CEIB3:

    def __init__(self, username, password, captcha_service=None):
        '''
        INPUTS: USERNAME (CPF WITHOUT . AND -), PASSWORD AND CAPTCHA SERVICE (NONE AS DEFAULT, 
            SINCE ITS NOT A MUST YET)
        '''
        self.username = username
        self.password = password
        self.captcha_service = captcha_service

    async def extract_data(self):
        '''
        REFERENCES: https://github.com/gicornachini/bolsa/blob/master/examples/simple.py
        DOCSTRING: EXTRACT DATA FROM CEI B3
        INPUTS: -
        OUTPUTS: ASSETS EXTRACTED IN A TUPLE
        '''
        # setting local variables
        dict_assets_extracted = dict()
        b3_httpclient = B3AsyncBackend(
            username=self.username,
            password=self.password,
            captcha_service=None  # captcha_service is not required yet
        )
        # defining brokers with available accounts in b3
        brokers = await b3_httpclient.get_brokers_with_accounts()
        dict_assets_extracted['brokers'] = brokers
        # deffining custody in each broker
        dict_assets_extracted['custody'] = (
            await b3_httpclient.get_brokers_account_portfolio_assets_extract(
                brokers=brokers
            )
        )
        # closing connection
        await b3_httpclient.session_close()
        await b3_httpclient.connection_close()
        # returning available brokers and custody in each one
        return dict_assets_extracted

    def assets_under_custody(self):
        '''
        DOCSTRING: ASSETS UNDER CUSTODY IN CEI B3
        INPUTS: -
        OUTPUTS: DICTIONARY
        '''
        # setting local variables
        dict_export = dict()
        dict_assets = dict()
        i_asset = 0
        # async threading to run extract data function inside CEI B3 class
        if __name__ == '__main__':
            # openning event loop
            loop = asyncio.get_event_loop()
            # running extract data function
            dict_assets_extracted = loop.run_until_complete(
                CEIB3(self.username, self.password, self.captcha_service).extract_data())
            # placing custody in each available broker
            dict_assets_extracted['reference_date'] = dict_assets_extracted[
                'brokers'][0].parse_extra_data.end_date
            for i in range(len(dict_assets_extracted['custody'])):
                # defining broker number in list
                if dict_assets_extracted['brokers'][i].name != '1982 - MODAL DTVM LTDA':
                    i_broker = i
                else:
                    i_broker = i + 1
                # broker info
                dict_export[dict_assets_extracted['brokers'][i_broker].name] = {
                    'broker_number': dict_assets_extracted['brokers'][i_broker].value,
                    'account': dict_assets_extracted['brokers'][i_broker].accounts[0].id,
                }
                # checking wheter the custody list is empty, and continue otherwise
                if str(dict_assets_extracted['custody'][i]) != '[]':
                    # extracting assets infos of the current broker
                    for asset in dict_assets_extracted['custody'][i]:
                        # since its possible to have several operations in the same asset, place
                        #   the ticker with a counter
                        if asset.raw_negotiation_code + '_' + str(i_asset) not in \
                                dict_assets.keys():
                            # reseting local variable
                            i_asset = 0
                            # creating key name for the main dictionary
                            key = asset.raw_negotiation_code + \
                                '_' + str(i_asset)
                        else:
                            i_asset += 1
                            key = asset.raw_negotiation_code + \
                                '_' + str(i_asset)
                        # asset infos
                        dict_assets[key] = {
                            'asset_name': asset.asset_specification,
                            'market_type': asset.market_type,
                            'operation_date': DatesBR().datetime_to_string(asset.operation_date),
                            'action': asset.action,
                            'quantity': asset.unit_amount,
                            'price': float(asset.unit_price),
                            'notional': float(asset.total_price),
                            'quotation_factor': asset.quotation_factor
                        }
                    # placing in export dictionary infos of the assets under management in
                    #   the current broker, available for the user account
                    dict_export[dict_assets_extracted['brokers'][i + 1].name]['custody'] = \
                        dict_assets
                    # exit loop if its in Modal DTVM due to its custody return being denied
                    if dict_assets_extracted['brokers'][i].name == '1982 - MODAL DTVM LTDA':
                        break
                else:
                    dict_export['custody'] = dict_assets_extracted['custody'][i]
            # returning required data
            return dict_export

    def earnings(self):
        '''
        REFERENCES: https://github.com/wagnervielmond/ProventosB3/blob/main/getProventosCEI.py
        DOCSTRING: EARNINGS RECEIVED IN BRAZILLIAN STOCK MARKET
        INPUTS: -
        OUTPUTS: DICTIONARY
        '''
        # setting local variables
        dict_export = dict()
        dict_assets = dict()
        i_asset = 0
        # async threading to run extract data function inside CEI B3 class
        if __name__ == '__main__':
            # openning event loop
            loop = asyncio.get_event_loop()
            # running extract data function
            dict_assets_extracted = loop.run_until_complete(
                CEIB3(self.username, self.password, self.captcha_service).extract_data())
            # placing custody in each available broker
            dict_assets_extracted['reference_date'] = dict_assets_extracted[
                'brokers'][0].parse_extra_data.end_date
            for i in range(len(dict_assets_extracted['custody'])):
                # defining broker number in list
                if dict_assets_extracted['brokers'][i].name != '1982 - MODAL DTVM LTDA':
                    i_broker = i
                else:
                    i_broker = i + 1
                # broker info
                dict_export[dict_assets_extracted['brokers'][i_broker].name] = {
                    'broker_number': dict_assets_extracted['brokers'][i_broker].value,
                    'account': dict_assets_extracted['brokers'][i_broker].accounts[0].id,
                }
                # checking wheter the custody list is empty, and continue otherwise
                if str(dict_assets_extracted['custody'][i]) != '[]':
                    # extracting assets infos of the current broker
                    for asset in dict_assets_extracted['custody'][i]:
                        # since its possible to have several operations in the same asset, place
                        #   the ticker with a counter
                        if asset.raw_negotiation_code + '_' + str(i_asset) not in \
                                dict_assets.keys():
                            # reseting local variable
                            i_asset = 0
                            # creating key name for the main dictionary
                            key = asset.raw_negotiation_code + \
                                '_' + str(i_asset)
                        else:
                            i_asset += 1
                            key = asset.raw_negotiation_code + \
                                '_' + str(i_asset)
                        # asset infos
                        dict_assets[key] = {
                            'asset_name': asset.asset_specification,
                            'raw_negotiation_code': asset.raw_negotiation_code,
                            'operation_date': DatesBR().datetime_to_string(asset.operation_date),
                            'event_type': asset.event_type,
                            'unit_amount': asset.unit_amount,
                            'quotation_factor': int(asset.quotation_factor),
                            'bruto_price': float(asset.bruto_price),
                            'liquido_price': asset.liquido_price
                        }
                    # placing in export dictionary infos of the assets under management in
                    #   the current broker, available for the user account
                    dict_export[dict_assets_extracted['brokers'][i + 1].name]['custody'] = \
                        dict_assets
                    # exit loop if its in Modal DTVM due to its custody return being denied
                    if dict_assets_extracted['brokers'][i].name == '1982 - MODAL DTVM LTDA':
                        break
                else:
                    dict_export['custody'] = dict_assets_extracted['custody'][i]
            # returning required data
            return dict_export
