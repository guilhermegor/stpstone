"""Brazilian 'Come-Cotas' Tax Calculation Module.

This module implements the semi-annual tax collection mechanism ('come-cotas') 
applicable to certain types of Brazilian investment funds, as regulated by 
Instruction 555 of the Brazilian Securities Commission (CVM).
"""

from datetime import date
from typing import Literal, Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class ComeCotasCalculator(metaclass=TypeChecker):
    """Calculator for Brazilian 'come-cotas' tax on investment funds.
    
    The 'come-cotas' is a semi-annual tax collection mechanism that anticipates
    Income Tax (IR) on earnings from certain types of investment funds in Brazil.
    
    Parameters
    ----------
    str_fund_type : str
        Type of investment fund (must be one of the supported types)
        
    Attributes
    ----------
    FUND_TYPES : dict
        Dictionary mapping fund types to their tax rates and characteristics
    """
    
    # supported fund types and their tax characteristics
    FUND_TYPES = {
        "FIA": {
            "description": "Ações (Stock Funds) - Minimum 67% in equities",
            "tax_rate": 0.15,
            "taxation_day": (5, 20, 11, 20),
            "has_come_cotas": False
        },
        "FIRF": {
            "description": "Referenciado (Benchmark Funds) - Tracks specific benchmarks",
            "tax_rate": 0.20,
            "taxation_day": (5, 20, 11, 20),
            "has_come_cotas": True
        },
        "FIM": {
            "description": "Multimercado (Multimarket Funds) - Flexible portfolios",
            "tax_rate": 0.20,
            "taxation_day": (5, 20, 11, 20),
            "has_come_cotas": True
        },
        "FIDC": {
            "description": "Direitos Creditórios (Credit Rights Funds)",
            "tax_rate": 0.20,
            "taxation_day": (5, 20, 11, 20),
            "has_come_cotas": False
        },
        "FIP": {
            "description": "Participações (Private Equity Funds)",
            "tax_rate": 0.15,
            "taxation_day": None,
            "has_come_cotas": False
        },
        "FI-IE": {
            "description": "Investimento no Exterior (Foreign Investment Funds)",
            "tax_rate": 0.15,
            "taxation_day": (5, 20, 11, 20),
            "has_come_cotas": True
        },
        "FII": {
            "description": "Imobiliário (Real Estate Funds)",
            "tax_rate": 0.20,
            "taxation_day": None,
            "has_come_cotas": False
        }
    }
    
    def __init__(
        self, 
        str_fund_type: Literal["FIA", "FIRF", "FIM", "FIDC", "FIP", "FI-IE", "FII"]
    ) -> None:
        """Initialize the calculator with a specific fund type."""
        if str_fund_type not in self.FUND_TYPES:
            raise ValueError(f"Invalid fund type. Must be one of: {list(self.FUND_TYPES.keys())}")
            
        self.str_fund_type = str_fund_type
        self.fund_data = self.FUND_TYPES[str_fund_type]
    
    def _validate_inputs(
        self,
        float_position_value: Optional[float] = None,
        float_acquisition_basis: Optional[float] = None,
        dt_ref: Optional[date] = None
    ) -> None:
        """Validate input types for tax calculation.
        
        Parameters
        ----------
        float_position_value : float, optional
            Position value to validate
        float_acquisition_basis : float, optional
            Acquisition basis to validate
        dt_ref : date, optional
            Date to validate
            
        Raises
        ------
        TypeError
            If any input has incorrect type
        """
        if float_position_value is not None and not isinstance(float_position_value, (float, int)):
            raise TypeError("Position value must be a float")
        if float_acquisition_basis is not None \
            and not isinstance(float_acquisition_basis, (float, int)):
            raise TypeError("Acquisition basis must be a float")
        if dt_ref is not None and not isinstance(dt_ref, date):
            raise TypeError("Date must be a date object")
        
    def is_tax_day(
        self, 
        dt_ref: Optional[date] = None
    ) -> bool:
        """Check if a given date is a 'come-cotas' collection day.
        
        Parameters
        ----------
        dt_ref : date, optional
            Date to check (defaults to current date)
            
        Returns
        -------
        bool
            True if the date is a tax collection day, False otherwise
        """
        if not self.fund_data["has_come_cotas"] or not self.fund_data["taxation_day"]:
            return False

        # if dt_ref is falsy (None, 0, False, "", [], etc.), it uses date.today()
        # only keeps dt_ref if it is truthy (e.g., a valid date, 1, True, "hello")
        dt_ref = dt_ref or date.today()
        tax_days = self.fund_data["taxation_day"]
        
        # check both May and November tax days
        return ((dt_ref.month == tax_days[0] and dt_ref.day == tax_days[1]) or
                (len(tax_days) > 2 and dt_ref.month == tax_days[2] and dt_ref.day == tax_days[3]))
    
    def calculate_tax(
        self,
        float_position_value: float,
        float_acquisition_basis: float,
        dt_ref: Optional[date] = None
    ) -> float:
        """Calculate the 'come-cotas' tax amount.
        
        Parameters
        ----------
        float_position_value : float
            Current value of the investment position
        float_acquisition_basis : float
            Original acquisition cost (for profit calculation)
        dt_ref : date, optional
            Date for tax calculation (defaults to current date)
            
        Returns
        -------
        float
            Tax amount due (0 if not a tax day or fund type doesn't apply)
            
        Raises
        ------
        ValueError
            If float_position_value is less than float_acquisition_basis
        """
        if not self.fund_data["has_come_cotas"]:
            return 0.0
            
        if float_position_value < float_acquisition_basis:
            raise ValueError("Position value cannot be less than acquisition cost")
            
        if not self.is_tax_day(dt_ref):
            return 0.0
            
        taxable_profit = float_position_value - float_acquisition_basis
        return taxable_profit * self.fund_data["tax_rate"]
    
    def get_fund_description(self) -> str:
        """Get the description of the fund type.
        
        Returns
        -------
        str
            Description of the fund type and its characteristics
        """
        return self.fund_data["description"]
    
    @classmethod
    def get_supported_str_fund_types(cls) -> list[str]:
        """Get list of all supported fund types.
        
        Returns
        -------
        list[str]
            List of fund type codes
        """
        return list(cls.FUND_TYPES.keys())