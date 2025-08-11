"""API interface for B3 Margin Simulator calculations.

This module provides a class for interacting with the B3 Margin Simulator API to calculate
total deficit/surplus values based on portfolio positions. It handles authentication,
request formatting, and response processing.
"""

from typing import Union

import numpy as np
from requests import request

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.json import JsonFiles


class MarginSimulatorB3(metaclass=TypeChecker):
    """Class for interacting with B3 Margin Simulator API."""

    def _validate_dict_payload(self, dict_payload: list[dict]) -> None:
        """Validate the input portfolio payload.

        Parameters
        ----------
        dict_payload : list[dict]
            Portfolio positions data to validate

        Raises
        ------
        ValueError
            If dict_payload is empty
            If any position entry is missing required fields
        """
        if not dict_payload:
            raise ValueError("Portfolio payload cannot be empty")
        for position in dict_payload:
            if not all(
                key in position
                for key in ["Security", "SecurityGroup", "Position"]
            ):
                raise ValueError("Each position must contain Security, SecurityGroup and Position")

    def __init__(
        self,
        dict_payload: list[dict],
        token: str,
        host: str = "https://simulador.b3.com.br/api/cors-app",
    ) -> None:
        """Initialize the margin simulator instance.

        Parameters
        ----------
        dict_payload : list[dict]
            Portfolio positions data. Example:
            [
                {
                    'Security': {'symbol': 'ABCBF160'},
                    'SecurityGroup': {'positionTypeCode': 0},
                    'Position': {
                        'longQuantity': 100,
                        'shortQuantity': 0,
                        'longPrice': 0,
                        'shortPrice': 0
                    }
                },
                ...
            ]
        token : str
            API authentication token (default: test token)
        host : str, optional
            Base API host URL (default: production simulator URL)
        """
        self._validate_dict_payload(dict_payload)
        self.dict_payload = dict_payload
        self.token = token
        self.host = host

    def total_deficit_surplus(
        self,
        value_liquidity_resource: np.int64 = 4_700_000_000,
        timeout: Union[tuple[float, float], float, int] = (200, 200)
    ) -> list[dict[str, Union[str, int, float]]]:
        """Calculate total deficit/surplus using B3 Margin Simulator API.

        Parameters
        ----------
        value_liquidity_resource : np.int64
            Liquidity resource value (default: 4,700,000,000)
        timeout : Union[tuple[float, float], float, int], optional
            Request timeout (default: (200, 200))

        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            list of calculation results from API

        Raises
        ------
        ValueError
            If API request fails or returns error

        References
        ----------
        .. [1] https://simulador.b3.com.br/
        """
        dict_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        dict_payload = {
            "ReferenceData": {"referenceDataToken": self.token},
            "LiquidityResource": {"value": value_liquidity_resource},
            "RiskPositionList": self.dict_payload
        }

        dict_payload = JsonFiles().dict_to_json(dict_payload)

        try:
            resp_req = request(
                method="POST",
                url=self.host + "/web/V1.0/RiskCalculation",
                headers=dict_headers,
                data=dict_payload,
                verify=False, 
                timeout=timeout
            )
            resp_req.raise_for_status()
            return resp_req.json()
        except Exception as err:
            raise ValueError(f"API request failed: {str(err)}") from err