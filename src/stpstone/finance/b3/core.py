### API TO GRANT ACCESS TO B3 MARGIN SIMULATOR ###

from requests import request


class MarginSimulatorB3:
    def __init__(
        self,
        client_id,
        client_secret,
        token=None,
        hostname_api_margin_simulator_b3='https://api.b3.com.br/cors-app',
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = token
        self.hostname_api_margin_simulator_b3 = (
            hostname_api_margin_simulator_b3
        )

    @property
    def access_token(
        self,
        method='GET',
        bl_verify=False,
        bl_str_dict_params=False,
        app='/api/oauth/token',
    ):
        """
        DOCSTRING: ACCESS TOKEN TO GRANT ACCESS TO MARGIN SIMULATOR B3
        INPUTS: METHOD (DEFAULT), KEY HEADER (DEFAULT), URL AUTHENTIFICATION
            HEADER (DEFAULT)
        OUTPUTS: STRING
        """
        # passing variables
        i = 0
        # requesting authorization authheader
        dict_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        # parameters
        dict_params = {
            'grant_type': 'client_credentials',
            'client_id': str(self.client_id),
            'client_secret': str(self.client_secret),
        }
        #   coverting to string, if its user will
        if bl_str_dict_params == True:
            dict_params = '&'.join(
                '{}={}'.format(k, v) for k, v in dict_params.items()
            )
        # looping while status code not a 2xx response
        resp_req = request(
            method=method,
            url=self.hostname_api_margin_simulator_b3 + app,
            headers=dict_headers,
            params=dict_params,
            verify=bl_verify,
        )
        # raises exception when not a 2xx response
        resp_req.raise_for_status()
        # getting authheader
        return resp_req.json()
