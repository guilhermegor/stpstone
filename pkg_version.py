#!/usr/bin/env python3
import sys
import json
import toml
from pathlib import Path

def get_package_info():
    """Detect package name and version from pyproject.toml, supporting both Poetry and PEP 621 formats.
    If successful, returns the package information. If unsuccessful, prints an error message and exits
    with a non-zero status code.

    The function first checks for Poetry-style configuration ([tool.poetry] section), then falls back
    to PEP 621 standard ([project] section). The version in PEP 621 may be dynamic, in which case
    "0.0.0" is returned as a fallback.

    Parameters
    ----------
    None
        (This function takes no parameters as it reads directly from pyproject.toml)

    Returns
    -------
    dict
        A dictionary containing:
        - name : str
            The package name
        - version : str
            The package version
        - source : str
            Either "poetry" or "pep621" indicating which format was used

    Raises
    ------
    FileNotFoundError
        If pyproject.toml cannot be found
    ValueError
        If neither Poetry nor PEP 621 configuration is found
    toml.TomlDecodeError
        If pyproject.toml is not valid TOML
    """
    try:
        pyproject_path = Path("pyproject.toml")
        if not pyproject_path.exists():
            raise FileNotFoundError("pyproject.toml not found")
        
        data = toml.load(pyproject_path)
        
        if "tool" in data and "poetry" in data["tool"]:
            poetry_data = data["tool"]["poetry"]
            return {
                "name": poetry_data["name"],
                "version": poetry_data["version"],
                "source": "poetry"
            }
        
        # fallback to PEP 621 style
        # PEP 621 specifies that the project metadata should be under the [project] section
        if "project" in data:
            project_data = data["project"]
            return {
                "name": project_data["name"],
                "version": project_data.get("version", "0.0.0"),
                "source": "pep621"
            }
        
        raise ValueError("Neither [tool.poetry] nor [project] sections found in pyproject.toml")
        
    except Exception as e:
        sys.stderr.write(f"Error detecting package info: {str(e)}\n")
        sys.exit(1)

if __name__ == "__main__":
    try:
        info = get_package_info()
        print(json.dumps(info, indent=2))
    except Exception as e:
        sys.stderr.write(f"Failed to get package info: {str(e)}\n")
        sys.exit(1)