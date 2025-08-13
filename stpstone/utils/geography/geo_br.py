"""Geographical information utilities for Brazil.

This module provides methods to retrieve Brazilian states and zip code information
from public APIs, with options for accent handling and data formatting.
"""

import ast
from typing import Any, TypedDict

from requests import request

from stpstone.utils.parsers.str import StrHandler


class ReturnStates(TypedDict):
    """Typed dictionary for Brazilian states information.

    Attributes
    ----------
    id : int
        State identifier code
    sigla : str
        State abbreviation
    nome : str
        State name
    regiao : dict[str, Any]
        Region information containing id, sigla and nome
    """

    id: int
    sigla: str
    nome: str
    regiao: dict[str, Any]


class ReturnZipCode(TypedDict):
    """Typed dictionary for zip code information.

    Attributes
    ----------
    address_type : str
        Type of address
    address : str
        Full address
    state : str
        State abbreviation
    """

    address_type: str
    address: str
    state: str


class BrazilGeo:
    """Class for retrieving Brazilian geographical information."""

    def __init__(self) -> None:
        """Initialize the BrazilGeo class."""
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
            if not isinstance(zip_code, str):
                raise ValueError("All zip codes must be strings")
            if not zip_code.strip():
                raise ValueError("Zip code cannot be empty")

    def states(
        self,
        bol_accents: bool = True,
    ) -> list[ReturnStates]:
        """Return Brazilian states information.

        Parameters
        ----------
        bol_accents : bool, optional
            Whether to include accents in state names (default: True)
        
        Returns
        -------
        list[ReturnStates]
            list of dictionaries containing state information

        References
        ----------
        .. [1] https://servicodados.ibge.gov.br/api/v1/localidades/estados
        """
        url_localidades_brasil = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
        resp_req = request("GET", url_localidades_brasil)
        
        try:
            dict_ = ast.literal_eval(
                self.cls_str_handler.get_between(str(resp_req.text.encode("utf8")), "b'", "'")
            )
        except (ValueError, SyntaxError) as err:
            raise ValueError("Failed to parse API response") from err
        
        for state_info in dict_:
            state_info["nome"] = self.cls_str_handler.latin_characters(state_info["nome"])
            if not bol_accents:
                state_info["nome"] = self.cls_str_handler.removing_accents(state_info["nome"])
        
        return dict_

    def zip_code(
        self,
        list_zip_codes: list[str],
    ) -> dict[str, ReturnZipCode]:
        """Retrieve location information for zip codes.

        Parameters
        ----------
        list_zip_codes : list[str]
            list of zip codes to query

        Returns
        -------
        dict[str, ReturnZipCode]
            Dictionary mapping zip codes to their location information

        Raises
        ------
        ValueError
            If any request fails or resp_req parsing fails
        """
        self._validate_zip_codes(list_zip_codes)
        dict_ = {}

        for zip_code in list_zip_codes:
            try:
                resp_req = request(
                    method="GET", 
                    url=f"https://cep.awesomeapi.com.br/json/{zip_code}"
                )
                resp_req.raise_for_status()
                json_zip_codes = resp_req.json()
                dict_[zip_code] = {
                    "address_type": json_zip_codes["address_type"],
                    "address": json_zip_codes["address"],
                    "state": json_zip_codes["state"]
                }
            except Exception as err:
                raise ValueError(f"Failed to process zip code {zip_code}") from err

        return dict_