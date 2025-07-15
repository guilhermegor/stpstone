Please refactor the following Python code to strictly adhere to these requirements:

1. **Formatting Rules**:
   - Line length: 99 characters max
   - Indentation: 4-space tabs
   - Python version: 3.9+ compatible
   - Quotes: Double quotes for all strings and dictionaries
   - Docstrings: numpy style (with Parameters/Returns/References sections)
   - Type hints: Required for all function signatures and returns

2. **Linting Requirements**:
   - Follow numpy-style docstrings (79 char line length for docstring content)
   - Maintain all existing references in docstrings
   - Comply with flake8, pycodestyle, and mypy type checking
   - Use explicit type annotations (no implicit Any)
   - Remove all commented-out code
   - Sort imports properly (isort)

3. **Specific Instructions**:
   - Preserve all original functionality
   - Keep docstring references exactly as formatted
   - Use descriptive variable names
   - Add missing type hints where needed
   - Break long lines using Python's implied line continuation
   - Maintain consistent whitespace around operators
   - Use f-strings instead of .format() where possible

4. **Output Format**:
   - Return only the refactored code
   - No explanations or notes
   - Include all original imports
   - Maintain original file structure

5. **Example of Required Style**:
   ```python
   def sample_function(param1: int, param2: str) -> dict[str, str]:
       """
       Brief description of function.

       Parameters
       ----------
       param1 : int
           Description of first parameter
       param2 : str
           Description of second parameter

       Returns
       -------
       dict[str, str]
           Description of return value

       References
       ----------
       .. [1] https://example.com/reference
       """
       return {"key": param2 * param1}
    ```

Please refactor this code:

```python

<FILL_WITH_FEATURE_IMPLEMENTED>
```