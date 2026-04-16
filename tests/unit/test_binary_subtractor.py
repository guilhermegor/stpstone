"""Unit tests for the BinarySubtractor class implementation."""

import pytest

from stpstone.analytics.arithmetic.binary_subtractor import BinarySubtractor


class TestBinarySubtractor:
	"""Test cases for BinarySubtractor class."""

	@pytest.mark.parametrize(
		"minuend, subtrahend, expected",
		[
			("101", "010", "011"),  # 5 - 2 = 3
			("1101", "0110", "0111"),  # 13 - 6 = 7
			("1000", "0001", "0111"),  # 8 - 1 = 7
			("1111", "1111", "0000"),  # 15 - 15 = 0
		],
	)
	def test_subtraction_normal_cases(self, minuend: str, subtrahend: str, expected: str) -> None:
		"""Test binary subtraction with normal input cases.

		Parameters
		----------
		minuend : str
			The minuend (the number to be subtracted from)
		subtrahend : str
			The subtrahend (the number to be subtracted)
		expected : str
			The expected result of the subtraction
		"""
		sub: BinarySubtractor = BinarySubtractor(minuend, subtrahend)
		assert sub.subtract() == expected

	@pytest.mark.parametrize(
		"minuend, subtrahend, expected",
		[
			("10101", "110", "01111"),  # 21 - 6 = 15
			("110", "10101", "10001"),  # 6 - 21 = -15 (two's complement)
		],
	)
	def test_different_length_inputs(self, minuend: str, subtrahend: str, expected: str) -> None:
		"""Test subtraction with inputs of different lengths.

		This tests the case where the minuend and subtrahend have different lengths.

		Parameters
		----------
		minuend : str
			The minuend (the number to be subtracted from)
		subtrahend : str
			The subtrahend (the number to be subtracted)
		expected : str
			The expected result of the subtraction
		"""
		sub: BinarySubtractor = BinarySubtractor(minuend, subtrahend)
		assert sub.subtract() == expected

	# Edge Cases
	@pytest.mark.parametrize(
		"minuend, subtrahend, expected",
		[
			("0000", "0101", "1011"),  # 0 - 5 = -5 (two's complement)
			("1010", "0000", "1010"),  # 10 - 0 = 10
			("1", "0", "1"),  # 1 - 0 = 1
			("1", "1", "0"),  # 1 - 1 = 0
		],
	)
	def test_edge_cases(self, minuend: str, subtrahend: str, expected: str) -> None:
		"""Test binary subtraction with edge cases.

		Parameters
		----------
		minuend : str
			The minuend (the number to be subtracted from)
		subtrahend : str
			The subtrahend (the number to be subtracted)
		expected : str
			The expected result of the subtraction
		"""
		sub: BinarySubtractor = BinarySubtractor(minuend, subtrahend)
		assert sub.subtract() == expected

	# Error Conditions
	@pytest.mark.parametrize(
		"minuend, subtrahend",
		[
			("", "101"),  # empty minuend
			("101", ""),  # empty subtrahend
			("", ""),  # both empty
		],
	)
	def test_empty_strings(self, minuend: str, subtrahend: str) -> None:
		"""Test with empty input strings.

		Parameters
		----------
		minuend : str
			The minuend (the number to be subtracted from)
		subtrahend : str
			The subtrahend (the number to be subtracted)
		"""
		with pytest.raises(ValueError):
			BinarySubtractor(minuend, subtrahend)

	@pytest.mark.parametrize(
		"minuend, subtrahend",
		[
			("1021", "0101"),  # invalid digit in minuend
			("1010", "01x1"),  # invalid digit in subtrahend
			("abc", "101"),  # non-binary characters
		],
	)
	def test_invalid_binary_strings(self, minuend: str, subtrahend: str) -> None:
		"""Test with invalid binary strings.

		Parameters
		----------
		minuend : str
			The minuend (the number to be subtracted from)
		subtrahend : str
			The subtrahend (the number to be subtracted)
		"""
		with pytest.raises(ValueError):
			BinarySubtractor(minuend, subtrahend)

	@pytest.mark.parametrize(
		"minuend, subtrahend",
		[
			(101, "0101"),  # minuend not string
			("1010", 101),  # subtrahend not string
			(True, "1010"),  # boolean minuend
			("1010", None),  # none subtrahend
		],
	)
	def test_type_validation(self, minuend: object, subtrahend: object) -> None:
		"""Test type validation of inputs.

		Parameters
		----------
		minuend : object
			The minuend (the number to be subtracted from)
		subtrahend : object
			The subtrahend (the number to be subtracted)
		"""
		with pytest.raises(TypeError):
			BinarySubtractor(minuend, subtrahend)

	@pytest.mark.parametrize(
		"minuend, subtrahend, expected",
		[
			("10000", "00001", "01111"),  # 16 - 1 = 15
			("1010101", "0101010", "0101011"),  # 85 - 42 = 43
		],
	)
	def test_borrow_propagation(self, minuend: str, subtrahend: str, expected: str) -> None:
		"""Test proper borrow propagation in subtraction.

		Parameters
		----------
		minuend : str
			The minuend (the number to be subtracted from)
		subtrahend : str
			The subtrahend (the number to be subtracted)
		expected : str
			The expected result of the subtraction
		"""
		sub: BinarySubtractor = BinarySubtractor(minuend, subtrahend)
		assert sub.subtract() == expected

	def test_result_storage(self) -> None:
		"""Test that result is properly stored in the instance."""
		sub: BinarySubtractor = BinarySubtractor("1101", "0110")  # 13 - 6
		result: str = sub.subtract()
		assert sub.result == "0111"
		assert result == sub.result

	@pytest.mark.parametrize(
		"minuend, subtrahend, expected",
		[
			("0101", "1010", "1011"),  # 5 - 10 = -5 (two's complement)
			("0", "1", "1"),  # 0 - 1 = -1 (two's complement)
		],
	)
	def test_negative_results(self, minuend: str, subtrahend: str, expected: str) -> None:
		"""Test cases that produce negative results.

		Parameters
		----------
		minuend : str
			The minuend (the number to be subtracted from)
		subtrahend : str
			The subtrahend (the number to be subtracted)
		expected : str
			The expected result of the subtraction
		"""
		sub: BinarySubtractor = BinarySubtractor(minuend, subtrahend)
		assert sub.subtract() == expected
