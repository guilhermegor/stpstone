"""Geographical information utilities for Brazil.

This module provides methods to retrieve Brazilian states and zip code information
from public APIs, with options for accent handling and data formatting.
"""

from time import sleep
from typing import TypedDict

import requests
from requests.exceptions import HTTPError, Timeout

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.str import StrHandler


class RegionResult(TypedDict, metaclass=TypeChecker):
    """Typed dictionary for region information.
    
    Attributes
    ----------
    id : int
        RegionResult ID
    sigla : str
        RegionResult abbreviation
    nome : str
        RegionResult name
    """

    id: int
    sigla: str
    nome: str


class ReturnStates(TypedDict, metaclass=TypeChecker):
    """Typed dictionary for Brazilian states information.

    Attributes
    ----------
    id : int
        State ID
    sigla : str
        State abbreviation
    nome : str
        State name
    regiao : RegionResult
        RegionResult information
    """
    
    id: int
    sigla: str
    nome: str
    regiao: RegionResult


class ReturnZipCode(TypedDict, metaclass=TypeChecker):
    """Typed dictionary for zip code information.

    Attributes
    ----------
    cep : str
        Zip code
    address_type : str
        Type of address
    address_name : str
        Name part of address
    address : str
        Full address
    state : str
        State abbreviation
    district : str
        District name
    lat : str
        Latitude
    long : str
        Longitude
    city : str
        City name
    city_ibge : str
        IBGE city code
    ddd : str
        Area code
    """

    cep: str
    address_type: str
    address_name: str
    address: str
    state: str
    district: str
    lat: str
    long: str
    city: str
    city_ibge: str
    ddd: str


class BrazilGeo(metaclass=TypeChecker):
    """Class for retrieving Brazilian geographical information."""

    def __init__(self) -> None:
        """Initialize the BrazilGeo class.
        
        Returns
        -------
        None
        """
        self.cls_str_handler = StrHandler()

    def _validate_zip_codes(self, list_zip_codes: list[str]) -> None:
        """Validate list of zip codes.

        Parameters
        ----------
        list_zip_codes : list[str]
            list of zip codes to validate

        Raises
        ------
        ValueError
            If list is empty
            If any zip code is not a string
            If any zip code is empty
        """
        if not list_zip_codes:
            raise ValueError("Zip code list cannot be empty")
        for zip_code in list_zip_codes:
            if not zip_code.strip():
                raise ValueError("Zip code cannot be empty")

    def states(self) -> list[ReturnStates]:
        """Return Brazilian states information.

        Returns
        -------
        list[ReturnStates]
            list of dictionaries containing state information

        Raises
        ------
        HTTPError
            If there's an HTTP error from the API
        Timeout
            If the request times out

        References
        ----------
        .. [1] https://servicodados.ibge.gov.br/api/v1/localidades/estados
        """
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
        try:
            resp_req = requests.get(url, timeout=10)
            resp_req.raise_for_status()
            return resp_req.json()
        except requests.exceptions.Timeout as e:
            raise Timeout("Request timed out") from e
        except requests.exceptions.HTTPError as e:
            raise HTTPError(f"Server error: {e}") from e

    def zip_code(self, list_zip_codes: list[str]) -> dict[str, ReturnZipCode]:
        """Retrieve location information for zip codes.

        Parameters
        ----------
        list_zip_codes : list[str]
            list of zip codes to query

        Returns
        -------
        dict[str, ReturnZipCode]
            Dictionary mapping zip codes to their location information

        References
        ----------
        .. [1] https://cep.awesomeapi.com.br
        """
        self._validate_zip_codes(list_zip_codes)
        result = {}

        for zip_code in list_zip_codes:
            try:
                resp_req = requests.get(
                    f"https://cep.awesomeapi.com.br/json/{zip_code}",
                    timeout=10
                )
                resp_req.raise_for_status()
                result[zip_code] = resp_req.json()
                sleep(1)
            except (HTTPError, Timeout):
                result[zip_code] = {
                    "cep": "ERROR",
                    "address_type": "ERROR",
                    "address_name": "ERROR",
                    "address": "ERROR",
                    "state": "ERROR",
                    "district": "ERROR",
                    "lat": "ERROR",
                    "long": "ERROR",
                    "city": "ERROR",
                    "city_ibge": "ERROR",
                    "ddd": "ERROR"
                }
                continue

        return result