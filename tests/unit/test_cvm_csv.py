"""Unit tests for the CVM open-data CSV reader.

Tests the ``read_cvm_csv`` helper, including:
- Lossless parsing of a clean `;`-delimited CSV
- Robust parsing of a row with an unescaped double quote (no column shift, no row loss)
- Forwarding of keyword arguments to ``pandas.read_csv`` (e.g. ``dtype``)
- Type validation of the input parameter
"""

from io import StringIO

import pytest

from stpstone.utils.parsers.cvm_csv import read_cvm_csv


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def clean_csv() -> StringIO:
	"""Fixture providing a clean CVM-shaped CSV with no quoting.

	Returns
	-------
	StringIO
		Header plus two well-formed rows.
	"""
	return StringIO("COD;NAME;PRAZO\n1;alpha;10\n2;beta;20\n")


@pytest.fixture
def unescaped_quote_csv() -> StringIO:
	"""Fixture providing a CSV whose second data row has an unescaped double quote.

	The stray quote in the free-text ``NAME`` column is exactly the malformed-row pattern
	CVM administrators occasionally submit. Default pandas quoting would treat it as a field
	wrapper and shift the trailing numeric column.

	Returns
	-------
	StringIO
		Header plus one clean row and one row with an unescaped double quote.
	"""
	return StringIO('COD;NAME;PRAZO\n1;alpha;10\n2;IPCA"deliberation;20\n')


# --------------------------
# Tests
# --------------------------
def test_read_cvm_csv_clean_file_parses_losslessly(clean_csv: StringIO) -> None:
	"""Test reading a clean CSV preserves every row and column.

	Verifies
	--------
	- The shape matches the header width and the number of data rows
	- The numeric column parses numerically

	Parameters
	----------
	clean_csv : StringIO
		Clean CSV fixture
	"""
	df_ = read_cvm_csv(clean_csv)

	assert df_.shape == (2, 3)
	assert list(df_.columns) == ["COD", "NAME", "PRAZO"]
	assert df_["PRAZO"].tolist() == [10, 20]


def test_read_cvm_csv_unescaped_quote_keeps_row_and_columns(
	unescaped_quote_csv: StringIO,
) -> None:
	"""Test an unescaped double quote does not shift columns or drop the row.

	Verifies
	--------
	- The malformed row is parsed, not skipped (no row loss)
	- The field count stays the header width (no column shift)
	- The stray quote survives as literal text
	- The trailing numeric column parses numerically

	Parameters
	----------
	unescaped_quote_csv : StringIO
		CSV fixture with an unescaped double quote
	"""
	df_ = read_cvm_csv(unescaped_quote_csv)

	assert df_.shape == (2, 3)
	assert df_.iloc[1]["NAME"] == 'IPCA"deliberation'
	assert df_["PRAZO"].tolist() == [10, 20]


def test_read_cvm_csv_forwards_dtype_kwarg(clean_csv: StringIO) -> None:
	"""Test caller keyword arguments are forwarded to pandas.read_csv.

	Verifies
	--------
	- A ``dtype=str`` kwarg makes every column read as string

	Parameters
	----------
	clean_csv : StringIO
		Clean CSV fixture
	"""
	df_ = read_cvm_csv(clean_csv, dtype=str)

	assert df_["PRAZO"].tolist() == ["10", "20"]


def test_read_cvm_csv_non_stringio_input_raises_type_error() -> None:
	"""Test passing a non-StringIO input raises TypeError.

	Verifies
	--------
	- The ``@type_checker`` guard rejects a wrong-typed ``file`` argument

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		read_cvm_csv(123)  # type: ignore[arg-type]
