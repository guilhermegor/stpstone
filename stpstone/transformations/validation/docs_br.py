"""Brazilian document number validation and processing utilities.

This module provides a class for validating, masking, and retrieving public
information for various Brazilian document numbers using validate_docbr, requests,
and Pydantic for data validation.
"""

from datetime import datetime
import re
import time
from typing import Literal, Optional, TypeVar, Union

from pydantic import BaseModel, Field, field_validator
from requests import request
from validate_docbr import CNH, CNPJ, CNS, CPF, PIS, RENAVAM, TituloEleitoral

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


TypeDoc = TypeVar("TypeDoc", bound=Literal[
    "CNPJ",
    "CPF",
    "CNS",
    "PIS",
    "TITULO_ELEITORAL",
    "RENAVAM",
    "CNH",
])


class Atividade(BaseModel):
    """Pydantic model for atividade_principal or atividades_secundarias entries."""

    code: str
    text: str


class QSA(BaseModel):
    """Pydantic model for qsa entries."""

    nome: str
    qual: str


class Simples(BaseModel):
    """Pydantic model for simples entries."""

    optante: bool
    data_opcao: Optional[str]
    data_exclusao: Optional[str]
    ultima_atualizacao: str


class Simei(BaseModel):
    """Pydantic model for simei entries."""

    optante: bool
    data_opcao: Optional[str]
    data_exclusao: Optional[str]
    ultima_atualizacao: str


class Billing(BaseModel):
    """Pydantic model for billing entries."""

    free: bool
    database: bool


class ResultCNPJInfo(BaseModel):
    """Pydantic model for CNPJ public information response."""

    abertura: str = Field(..., pattern=r"^\d{2}/\d{2}/\d{4}$")
    situacao: str
    tipo: str
    nome: str
    fantasia: str
    porte: str
    natureza_juridica: str
    atividade_principal: list[Atividade]
    atividades_secundarias: list[Atividade]
    qsa: list[QSA]
    logradouro: str
    numero: str
    municipio: str
    bairro: str
    uf: str
    cep: str
    telefone: str
    data_situacao: str
    cnpj: str
    ultima_atualizacao: str
    status: str
    complemento: str
    email: str
    efr: str
    motivo_situacao: str
    situacao_especial: str
    data_situacao_especial: str
    capital_social: str
    simples: Simples
    simei: Simei
    extra: dict[str, object]
    billing: Billing

    @field_validator("abertura")
    @classmethod
    def validate_abertura_format(cls, v: str) -> str:
        """
        Validate that abertura is a valid date in DD/MM/AAAA format.

        Parameters
        ----------
        v : str
            The abertura date string to validate

        Returns
        -------
        str
            The validated date string

        Raises
        ------
        ValueError
            If the date string is not a valid date
        """
        try:
            day, month, year = map(int, v.split("/"))
            if not (1 <= day <= 31):
                raise ValueError(f"Invalid day in abertura: {day}")
            if not (1 <= month <= 12):
                raise ValueError(f"Invalid month in abertura: {month}")
            if not (1900 <= year <= 9999):
                raise ValueError(f"Invalid year in abertura: {year}")
            datetime.strptime(v, "%d/%m/%Y")
            return v
        except ValueError as err:
            raise ValueError(f"Invalid date format for abertura: {v}") from err


class DocumentsNumbersBR(metaclass=TypeChecker):
    """Handle validation and processing of Brazilian document numbers."""

    def __init__(self, list_docs: list[str]) -> None:
        """
        Initialize with list of document numbers.

        Parameters
        ----------
        list_docs : list[str]
            list of document numbers as strings
        """
        self.list_docs = list_docs
        self._validate_list_docs()

    def _validate_list_docs(self) -> None:
        """
        Validate list_docs structure and contents.

        Raises
        ------
        TypeError
            If list_docs is not a list
            If any element in list_docs is not a string
        ValueError
            If list_docs is empty
        """
        if not isinstance(self.list_docs, list):
            raise TypeError("list_docs must be of type list")
        if not all(isinstance(d, str) for d in self.list_docs):
            raise TypeError("All elements in list_docs must be of type str")
        if len(self.list_docs) == 0:
            raise ValueError("list_docs cannot be empty")
    
    def _get_doc_validator(
        self,
        doc: TypeDoc,
    ) -> Union[CNH, CNPJ, CNS, CPF, PIS, RENAVAM, TituloEleitoral]:
        """
        Get the validator class for a given document type.

        Parameters
        ----------
        doc : TypeDoc
            Type of document

        Returns
        -------
        Union[CNH, CNPJ, CNS, CPF, PIS, RENAVAM, TituloEleitoral]
            Validator class instance for the document type

        Raises
        ------
        ValueError
            If doc is invalid
        TypeError
            If doc is not a string
        """
        if not isinstance(doc, str):
            raise TypeError("doc must be of type str")

        doc_mapping = {
            "CNPJ": CNPJ,
            "CPF": CPF,
            "CNS": CNS,
            "PIS": PIS,
            "TITULO_ELEITORAL": TituloEleitoral,
            "RENAVAM": RENAVAM,
            "CNH": CNH,
        }
        
        try:
            return doc_mapping[doc.upper()]()
        except KeyError as err:
            raise ValueError(f"Invalid document type: {doc}") from err

    def _is_already_masked(
        self,
        doc_number: str,
        doc: TypeDoc,
    ) -> bool:
        """
        Check if a document number is already masked according to its type.

        Parameters
        ----------
        doc : TypeDoc
            Document type identifier
        doc_number : str
            Number to check for masking

        Returns
        -------
        bool
            True if the document number is already masked, False otherwise

        Raises
        ------
        ValueError
            If doc is invalid
        """
        mask_patterns = {
            "CNPJ": r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$",  # 12.345.678/9012-34
            "CPF": r"^\d{3}\.\d{3}\.\d{3}-\d{2}$",  # 123.456.789-01
            "CNS": r"^[1-2]\d{2}\s\d{4}\s\d{4}\s\d{4}$",  # 123 4567 8901 2345
            "PIS": r"^\d{3}\.\d{5}\.\d{2}-\d$",  # 123.45678.90-1
            "TITULO_ELEITORAL": r"^\d{4}\s\d{4}\s\d{4}$",  # 1234 5678 9012
            "RENAVAM": r"^\d{11}$",  # 12345678901 (no mask)
            "CNH": r"^\d{11}$",  # 12345678901 (no mask)
        }
        
        try:
            pattern = mask_patterns[doc.upper()]
            return bool(re.match(pattern, doc_number))
        except KeyError as err:
            raise ValueError(f"Invalid document type: {doc}") from err

    def validate_doc(
        self,
        doc: TypeDoc = "CNPJ",
    ) -> list[tuple[str, bool]]:
        """
        Validate list of document numbers.

        Parameters
        ----------
        doc : TypeDoc
            Type of document (default: "CNPJ")

        Returns
        -------
        list[tuple[str, bool]]
            list of tuples containing document number and validation result
        """
        validator = self._get_doc_validator(doc)
        list_results = validator.validate_list(self.list_docs)
        return [
            (self.list_docs[i], bool_valid_doc and self.list_docs[i] != "00000000000000")
            for i, bool_valid_doc in enumerate(list_results)
        ]

    def mask_numbers(
        self,
        doc: TypeDoc = "CNPJ",
    ) -> list[str]:
        """
        Mask list of document numbers according to type if not already masked.

        Parameters
        ----------
        doc : TypeDoc
            Type of document (default: "CNPJ")

        Returns
        -------
        list[str]
            Masked document numbers or original if already masked
        """
        validator = self._get_doc_validator(doc)
        list_results = []
        
        for doc_number in self.list_docs:
            if not doc_number:
                list_results.append("")
            elif self._is_already_masked(doc_number, doc):
                list_results.append(doc_number)
            else:
                list_results.append(validator.mask(doc_number))
        
        return list_results

    def unmask_docs(self) -> list[str]:
        """
        Remove formatting from document numbers.

        Returns
        -------
        list[str]
            Unmasked document numbers
        """
        return [
            d.replace(".", "").replace("/", "").replace("-", "").replace(" ", "")
            for d in self.list_docs
        ]

    def _validate_max_requests(self, max_requests_per_minute: int) -> None:
        """
        Validate max requests per minute value.

        Parameters
        ----------
        max_requests_per_minute : int
            Maximum requests per minute

        Raises
        ------
        ValueError
            If value is not positive
        """
        if max_requests_per_minute <= 0:
            raise ValueError("max_requests_per_minute must be positive")

    def get_public_info_cnpj(
        self,
        max_requests_per_minute: int = 3,
        timeout: Union[int, float, tuple[float, float], tuple[int, int]] = (12.0, 21.0),
    ) -> list[ResultCNPJInfo]:
        """
        Retrieve public information for CNPJ numbers with rate limiting.

        Parameters
        ----------
        max_requests_per_minute : int
            Maximum requests per minute (default: 3)
        timeout : Union[int, float, tuple[float, float], tuple[int, int]]
            Timeout for requests (default: (12.0, 21.0))

        Returns
        -------
        list[ResultCNPJInfo]
            list of CNPJ information Pydantic models

        Raises
        ------
        ValueError
            If request fails or invalid parameters
            If response data fails Pydantic validation

        References
        ----------
        .. [1] https://receitaws.com.br/v1/cnpj/60746948000112
        """
        self._validate_max_requests(max_requests_per_minute)
        url: str = "https://receitaws.com.br/v1/cnpj/{}"
        results = []
        cls_cnpj_validity = self.validate_doc("CNPJ")
        list_valid_cnpjs = [doc for doc, is_valid in cls_cnpj_validity if is_valid]
        
        for i, cnpj_number in enumerate(list_valid_cnpjs):
            try:
                response = request("GET", url.format(cnpj_number), timeout=timeout)
                response.raise_for_status()
                results.append(ResultCNPJInfo(**response.json()))
            except Exception as err:
                raise ValueError(
                    f"Failed to fetch or validate info for CNPJ {cnpj_number}: {str(err)}"
                ) from err
            if (i + 1) % max_requests_per_minute == 0 and i + 1 < len(
                self.list_docs
            ):
                time.sleep(60)
        return results