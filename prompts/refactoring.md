# Python Refactoring Specification

## Formatting Rules
- **Line length**: 99 characters maximum
- **Indentation**: 4 spaces (no tabs)
- **Python version**: 3.9+ compatibility required
- **Strings**: Double quotes for all strings and dictionaries
- **Docstrings**: numpy style with 79 character line limits, Parameters/Returns/Raises/Notes/References sections
- **Type hints**: 
  - Mandatory for all function signatures and returns
  - Please preserve data types that I include in the variable names
  - Preferably set apart specific validations that should be done accross methods in a class, like if len of an array is zero than raise a ValueError
    - This methods should have the naming format _validate_<NAME>
- **Typing imports**: 
  - Avoid `typing import Dict, Tuple, List` and affiliated, please resort to primitive ones, like dict, tuple, list, which would avoid ruff linting raising warnings
  - Use from numpy.typing import NDArray, NDArray[...] (e.g. NDArray[np.float64]) instead of np.ndarray for type hints
  - Use class Return<method_name>(TypedDict) for dictionaries typing (import from typing import TypedDict)
    - Docstring in numpy model for class Return<method_name>, skipping one row between it and the content below
- **Literals**: Use `Literal` for variables with limited options
- **Optionals**: Use `Optional` instead of `|` operator for python 3.9 compatibility

## Linting Requirements
- Strict numpy-style docstring compliance
- Preserve all references exactly as provided, in the references section of the docstring
- Allow URL breaks in docstrings when necessary
- Full compliance with:
  - flake8
  - pycodestyle 
  - mypy type checking
- Explicit type annotations (no implicit Any)
- Remove all commented-out code
- Proper import sorting using isort standards

## Implementation Requirements
1. **Functionality Preservation**:
   - Maintain all existing features
   - Keep original behavior unchanged

2. **Documentation**:
   - Preserve all docstring references exactly
   - Maintain any existing examples
   - Use descriptive variable names
   - Add missing type hints where needed
   - Create a description for the module at the beginning of it

3. **Code Style**:
   - Break long lines using Python's implied continuation
   - Consistent spacing around operators
   - Prefer f-strings over .format()
   - Proper numpy-style docstrings
   - Prefer the usage of " instead of ', where possible

4. **Validation Checks**:
   - _validate_<NAME> methods please include in the beginning of the class
   - Please add sanity checks for every variable within methods/functions that would be useful, with examples below
   - When using try except, use as err for error and from err in the implementation of raise, in order to avoid the Ruff error B904 Within an `except` clause, raise exceptions with `raise ... from err` or `raise ... from None` to distinguish them from errors in exception handling. 
      . Docuemnt the Raise reason, within the docstring in the appropriate section mentioned above
      . Please follow the example below:
```python
"""HTML data extraction utilities using lxml and requests.

This module provides a class for fetching and parsing HTML content from URLs
using lxml for XPath-based data extraction and requests for HTTP operations.
"""

from typing import Literal

from lxml import html
from requests import request


class HandlingLXML:
    """Class for handling HTML data extraction using lxml."""

    def _validate_url(self, url: str) -> None:
        """Validate URL format and content.

        Parameters
        ----------
        url : str
            URL to validate

        Raises
        ------
        ValueError
            If URL is empty 
            If URL is not a string
            If URL does not start with http:// or https://
        """
        if not url:
            raise ValueError("URL cannot be empty")
        if not isinstance(url, str):
            raise ValueError("URL must be a string")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("URL must start with http:// or https://")

    def fetch(self, url: str, method: Literal["get", "post"] = "get") -> html.ElementTree:
        """Fetch and parse HTML document for XPath selection.

        Parameters
        ----------
        url : str
            URL to fetch HTML content from
        method : Literal['get', 'post']
            HTTP request method (default: "get")

        Returns
        -------
        html.ElementTree
            Parsed HTML document

        Raises
        ------
        ValueError
            If URL is invalid or request fails

        References
        ----------
        .. [1] https://stackoverflow.com/questions/26944078/extracting-value-of-url-source-by-xpath-in-python
        """
        self._validate_url(url)
        try:
            content = request(method, url).content
            if not content:
                raise ValueError("Received empty response from URL")
            return html.fromstring(content)
        except Exception as err:
            raise ValueError(f"Failed to fetch or parse URL: {str(err)}") from err
```
   - **0-1 Range Values**:
     - Probabilities
     - P-values  
     - Correlation coefficients
     - Confidence levels
     - Normalized values
     - Activation function outputs
     - Rates/percentages

   - **Positive Numbers**:
     - Counts/quantities
     - Degrees of freedom
     - Sample sizes
     - Shape parameters
     - Physical measurements
     - Time values

   - **Negative Numbers**:
     - Negative coefficients
     - Logarithms (0 < x < 1)
     - Eigenvalues
     - Physical quantities
     - Financial losses

   - **Array Checks**:
     - Non-empty validation
     - Shape verification
     - Finite values check
     - Numeric type validation
     - Bounds checking

5. **Packages**:
    - Prefer packages already used in the project, instead of using new ones
    - Python libraries with score 80+ in the snyk Advisor
        - stpstone package is free from this restriction

## Output Format
- Return only the refactored code
- No additional explanations or notes
- Include all original imports
- Maintain exact file structure
- Preserve all functionality
- Do not return OK/NOK but True or False instead

## Example Implementation
```python
"""Data processing utilities for numerical arrays.

This module provides functions for validating and processing numerical data using NumPy.
It includes input validation and statistical computations with a focus on robust error handling.
"""

from typing import Optional, Literal
import numpy as np


def validate_input(value: float, name: str) -> None:
    """Validate that a value is between 0 and 1.

    Parameters
    ----------
    value : float
        Value to validate
    name : str
        Variable name for error messages

    Raises
    ------
    ValueError
        If value is outside [0, 1] range
    """
    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be between 0 and 1, got {value}")


def process_data(
    data: np.ndarray,
    method: Literal["mean", "median"] = "mean"
) -> dict[str, Optional[float]]:
    """Process numerical data using specified method.

    Parameters
    ----------
    data : np.ndarray
        Input data array (must not be empty)
    method : Literal["mean", "median"]
        Processing method (default: "mean")

    Returns
    -------
    dict[str, Optional[float]]
        Dictionary containing processed result

    References
    ----------
    .. [1] https://numpy.org/doc/stable/reference/generated/numpy.mean.html
    .. [2] https://numpy.org/doc/stable/reference/generated/numpy.median.html
    """
    if len(data) == 0:
        raise ValueError("Input array cannot be empty")
    if not np.all(np.isfinite(data)):
        raise ValueError("Input array contains NaN or infinite values")

    result = {
        "value": np.mean(data) if method == "mean" else np.median(data),
        "valid": True
    }
    return result
```

Please refactor this code:

```python

<FILL_WITH_FEATURE_IMPLEMENTED>
```