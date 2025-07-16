# Python Refactoring Specification

## Formatting Rules
- **Line length**: 99 characters maximum
- **Indentation**: 4 spaces (no tabs)
- **Python version**: 3.9+ compatibility required
- **Strings**: Double quotes for all strings and dictionaries
- **Docstrings**: numpy style with 79 character line limits, Parameters/Returns/Raises/Notes/References sections
- **Type hints**: Mandatory for all function signatures and returns
- **Typing imports**: 
  - Avoid `Dict`, `List`, `Tuple`
  - Use built-in `dict`, `list`, `tuple` instead
- **Literals**: Use `Literal` for variables with limited options
- **Optionals**: Use `Optional` instead of `|` operator

## Linting Requirements
- Strict numpy-style docstring compliance
- Preserve all references exactly as provided
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

3. **Code Style**:
   - Break long lines using Python's implied continuation
   - Consistent spacing around operators
   - Prefer f-strings over .format()
   - Proper numpy-style docstrings

4. **Validation Checks**:
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

## Output Format
- Return only the refactored code
- No additional explanations or notes
- Include all original imports
- Maintain exact file structure
- Preserve all functionality

## Example Implementation
```python
from typing import Optional, Literal
import numpy as np


def validate_input(value: float, name: str) -> None:
    """
    Validate that a value is between 0 and 1.

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
    """
    Process numerical data using specified method.

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