"""Unit tests for Resources — B3 LINE API market resources adapter.

Covers instrument-information retrieval and the ``instrument_infos_exchange_limits``
method that performs a left-join between instrument data and exchange limits via
pandas. All I/O is mocked; the pandas merge is exercised with synthetic data.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""


import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.resources import Resources


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_ops(mocker: MockerFixture) -> object:
	"""``MagicMock`` satisfying ``IOperations``.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Any
		Mocked operations adapter.
	"""
	ops = mocker.MagicMock()
	ops.app_request.return_value = [{"id": "1", "symbol": "PETR4", "name": "Petrobras"}]
	ops.exchange_limits.return_value = [
		{"instrumentId": "1", "symbol": "PETR4", "limitSpci": 1000, "limitSpvi": 2000}
	]
	return ops


@pytest.fixture
def resources(mock_ops: object) -> Resources:
	"""``Resources`` backed by a mocked operations adapter.

	Parameters
	----------
	mock_ops : object
		Mocked ``IOperations``.

	Returns
	-------
	Resources
		Ready-to-use adapter.
	"""
	return Resources(ops=mock_ops)


# --------------------------
# Tests
# --------------------------
class TestResources:
	"""Test suite for ``Resources``."""

	def test_init_stores_operations(self, mock_ops: object) -> None:
		"""Constructor stores the injected operations adapter.

		Parameters
		----------
		mock_ops : object
			Mocked operations.

		Returns
		-------
		None
		"""
		adapter = Resources(ops=mock_ops)
		assert adapter._ops is mock_ops

	def test_instrument_informations_calls_get_symbol_endpoint(
		self,
		resources: Resources,
		mock_ops: object,
	) -> None:
		"""``instrument_informations`` issues a GET to ``/api/v1.0/symbol`` with retry.

		Parameters
		----------
		resources : Resources
			Adapter under test.
		mock_ops : object
			Mocked operations.

		Returns
		-------
		None
		"""
		resources.instrument_informations()
		mock_ops.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/symbol",
			bool_retry_if_error=True,
		)

	def test_instrument_id_by_symbol_passes_symbol_in_path(
		self,
		resources: Resources,
		mock_ops: object,
	) -> None:
		"""``instrument_id_by_symbol`` encodes the symbol in the URL path.

		Parameters
		----------
		resources : Resources
			Adapter under test.
		mock_ops : object
			Mocked operations.

		Returns
		-------
		None
		"""
		resources.instrument_id_by_symbol(symbol="PETR4")
		mock_ops.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/symbol/PETR4",
		)

	def test_instrument_infos_exchange_limits_keys_result_by_symbol(
		self,
		resources: Resources,
	) -> None:
		"""``instrument_infos_exchange_limits`` returns a dict keyed by instrument symbol.

		Parameters
		----------
		resources : Resources
			Adapter under test.

		Returns
		-------
		None
		"""
		result = resources.instrument_infos_exchange_limits()
		assert "PETR4" in result

	def test_instrument_infos_exchange_limits_merges_instrument_and_limit_data(
		self,
		resources: Resources,
	) -> None:
		"""Merged row contains fields from both instrument info and exchange limits.

		Parameters
		----------
		resources : Resources
			Adapter under test.

		Returns
		-------
		None
		"""
		result = resources.instrument_infos_exchange_limits()
		row = result["PETR4"]
		assert row["name"] == "Petrobras"
		assert row["limitSpci"] == 1000
		assert row["limitSpvi"] == 2000

	def test_instrument_infos_exchange_limits_renames_symbol_column(
		self,
		resources: Resources,
	) -> None:
		"""After merge, ``symbol_x`` is renamed to ``symbol`` and ``symbol_y`` is dropped.

		Parameters
		----------
		resources : Resources
			Adapter under test.

		Returns
		-------
		None
		"""
		result = resources.instrument_infos_exchange_limits()
		row = result["PETR4"]
		assert "symbol" in row
		assert "symbol_x" not in row
		assert "symbol_y" not in row
