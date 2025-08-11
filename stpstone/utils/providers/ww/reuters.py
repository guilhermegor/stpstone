"""Reuters API client for fetching financial data.

This module provides a class for interacting with Reuters API to fetch tokens and currency quotes.
It handles authentication and data retrieval from Reuters financial endpoints.
"""

from typing import Literal, TypedDict, Union
from urllib.parse import urljoin

from requests import request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.object import HandlingObjects


class ReturnToken(TypedDict):
    """Typed dictionary for token response.

    Parameters
    ----------
    access_token : str
        Access token for API authentication
    token_type : str
        Type of the token
    expires_in : int
        Expiration time in seconds
    """

    access_token: str
    token_type: str
    expires_in: int


class Reuters(metaclass=TypeChecker):
    """Client for Reuters API data fetching.
    
    References
    ----------
    .. [1] https://www.reuters.com/markets/currencies
    """

    def _validate_api_key(self, api_key: str) -> None:
        """Validate API key format.

        Parameters
        ----------
        api_key : str
            API key to validate

        Raises
        ------
        ValueError
            If API key is empty
        """
        if not api_key:
            raise ValueError("API key cannot be empty")

    def _validate_device_id(self, deviceid: str) -> None:
        """Validate device ID format.

        Parameters
        ----------
        deviceid : str
            Device ID to validate

        Raises
        ------
        ValueError
            If device ID is empty
        """
        if not deviceid:
            raise ValueError("Device ID cannot be empty")

    def fetch_data(
        self,
        app: str,
        payload: dict = None,
        method: Literal["GET", "POST"] = "GET",
        endpoint: str = "https://apiservice.reuters.com/api/", 
        timeout: Union[tuple[float, float], float, int] = (200, 200)
    ) -> str:
        """Fetch data from Reuters API.

        Parameters
        ----------
        app : str
            Application endpoint path
        payload : dict, optional
            Request parameters (default: empty dict)
        method : Literal['GET', 'POST']
            HTTP method (default: 'GET')
        endpoint : str
            Base API endpoint (default: Reuters service endpoint)
        timeout : Union[tuple[float, float], float, int]
            Request timeout (default: (200, 200))

        Returns
        -------
        str
            Raw response text

        Raises
        ------
        ValueError
            If request fails or returns invalid response

        References
        ----------
        .. [1] https://www.reuters.com/markets/currencies
        """
        if payload is None:
            payload = {}
        try:
            response = request(
                method=method,
                url=urljoin(endpoint, app),
                params=payload,
                verify=False, 
                timeout=timeout
            )
            if not response.text:
                raise ValueError("Received empty response from API")
            return response.text
        except Exception as err:
            raise ValueError(f"Failed to fetch data: {str(err)}") from err

    def token(
        self,
        api_key: str,
        deviceid: str,
        app: str = "service/modtoken",
        method: Literal["GET", "POST"] = "GET"
    ) -> ReturnToken:
        """Get authentication token from Reuters API.

        Parameters
        ----------
        api_key : str
            API key for authentication
        deviceid : str
            Device identifier
        app : str
            Token endpoint path (default: 'service/modtoken')
        method : Literal['GET', 'POST']
            HTTP method (default: 'GET')

        Returns
        -------
        ReturnToken
            Dictionary containing token information

        References
        ----------
        .. [1] https://www.reuters.com/markets/currencies
        """
        self._validate_api_key(api_key)
        self._validate_device_id(deviceid)

        payload = {
            "method": "get",
            "format": "json",
            "callback": "getChartData",
            "apikey": api_key,
            "deviceid": deviceid
        }

        json_response = self.fetch_data(app, payload, method)
        return HandlingObjects().literal_eval_data(json_response, "getChartData(", ")")

    def quotes(
        self,
        currency: str,
        app: str = "getFetchQuotes/{}",
        endpoint: str = "https://www.reuters.com/companies/api/"
    ) -> dict:
        """Fetch currency quotes from Reuters API.

        Parameters
        ----------
        currency : str
            Currency code to fetch
        app : str
            Quotes endpoint template (default: 'getFetchQuotes/{}')
        endpoint : str
            Base API endpoint (default: Reuters companies endpoint)

        Returns
        -------
        dict
            Dictionary containing quote information

        Raises
        ------
        ValueError
            If currency is empty

        References
        ----------
        .. [1] https://www.reuters.com/markets/currencies
        """
        if not currency:
            raise ValueError("Currency cannot be empty")

        url = urljoin(endpoint, app.format(currency))
        response_text = self.fetch_data(url)
        return HandlingObjects().literal_eval_data(response_text)