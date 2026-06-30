"""CVM open-data CSV parsing utilities.

This module centralises how CVM `;`-delimited open-data CSV files are read, so the quoting
convention cannot drift across the individual ingestion readers that consume them.
"""

import csv
from io import StringIO
from typing import Any

import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import type_checker


@type_checker
def read_cvm_csv(file: StringIO, **kwargs: Any) -> pd.DataFrame:  # noqa: ANN401
	"""Read a CVM `;`-delimited open-data CSV.

	CVM open data never wraps fields in double quotes, and administrators sometimes submit
	an unescaped double quote in a free-text field. ``csv.QUOTE_NONE`` keeps a stray quote as
	a literal character so it cannot swallow the following ``;``-separated values and shift
	subsequent columns. Caller keyword arguments (``encoding``, ``dtype``, ...) override the
	defaults.

	Parameters
	----------
	file : StringIO
		The CSV content to parse.
	**kwargs : Any
		Extra keyword arguments forwarded to ``pandas.read_csv`` (e.g. ``encoding``, ``dtype``).

	Returns
	-------
	pd.DataFrame
		The parsed DataFrame.

	References
	----------
	.. [1] https://docs.python.org/3/library/csv.html#csv.QUOTE_NONE
	"""
	return pd.read_csv(file, sep=";", quoting=csv.QUOTE_NONE, **kwargs)
