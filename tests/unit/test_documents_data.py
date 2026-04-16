"""Unit tests for DocumentsData — B3 LINE API document data adapter.

Verifies correct delegation to ``IConnectionApi.app_request`` for every public
method, including the ``doc_profile`` helper that extracts a sub-dict from the
raw API response. All I/O is mocked.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
"""

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.providers.br.line_b3.documents_data import DocumentsData


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_conn(mocker: MockerFixture) -> object:
	"""``MagicMock`` satisfying ``IConnectionApi``.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	Any
		Mocked connection.
	"""
	conn = mocker.MagicMock()
	conn.broker_code = "0001"
	conn.category_code = "1"
	conn.app_request.return_value = [{"result": "ok"}]
	return conn


@pytest.fixture
def documents_data(mock_conn: object) -> DocumentsData:
	"""``DocumentsData`` backed by a mocked connection.

	Parameters
	----------
	mock_conn : object
		Mocked ``IConnectionApi``.

	Returns
	-------
	DocumentsData
		Ready-to-use adapter.
	"""
	return DocumentsData(conn=mock_conn)


# --------------------------
# Tests
# --------------------------
class TestDocumentsData:
	"""Test suite for ``DocumentsData``."""

	def test_init_stores_connection(self, mock_conn: object) -> None:
		"""Constructor stores the injected connection.

		Parameters
		----------
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		adapter = DocumentsData(conn=mock_conn)
		assert adapter._conn is mock_conn

	def test_doc_info_calls_get_with_correct_params(
		self,
		documents_data: DocumentsData,
		mock_conn: object,
	) -> None:
		"""``doc_info`` sends a GET to ``/api/v1.0/document`` with participant context.

		Parameters
		----------
		documents_data : DocumentsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		documents_data.doc_info(doc_code="12345678901")
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/document",
			dict_params={
				"participantCode": "0001",
				"pnpCode": "1",
				"documentCode": "12345678901",
			},
			bool_retry_if_error=True,
		)

	def test_block_unblock_doc_calls_post_with_is_blocked_flag(
		self,
		documents_data: DocumentsData,
		mock_conn: object,
	) -> None:
		"""``block_unblock_doc`` sends a POST with ``isBlocked=True``.

		Parameters
		----------
		documents_data : DocumentsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		documents_data.block_unblock_doc(doc_id="DOC001")
		mock_conn.app_request.assert_called_once_with(
			method="POST",
			app="/api/v1.0/document/DOC001",
			dict_params={"id": "DOC001", "isBlocked": True},
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)

	def test_update_profile_sends_profile_and_rmkt_fields(
		self,
		documents_data: DocumentsData,
		mock_conn: object,
	) -> None:
		"""``update_profile`` passes ``profileFull`` and ``rmktEvaluation`` in payload.

		Parameters
		----------
		documents_data : DocumentsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		documents_data.update_profile(doc_id="D1", doc_profile_id="3", int_rmkt_evaluation=2)
		mock_conn.app_request.assert_called_once_with(
			method="POST",
			app="/api/v1.0/document/D1",
			dict_payload={"id": "D1", "profileFull": 3, "rmktEvaluation": 2},
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)

	def test_doc_profile_extracts_profile_id_and_name(
		self,
		documents_data: DocumentsData,
		mock_conn: object,
	) -> None:
		"""``doc_profile`` returns a dict with ``profile_id`` and ``profile_name`` keys.

		Parameters
		----------
		documents_data : DocumentsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		mock_conn.app_request.return_value = {"profileFull": 7, "profileName": "Moderado"}
		result = documents_data.doc_profile(doc_id="D2")
		assert result == {"profile_id": 7, "profile_name": "Moderado"}

	def test_spxi_get_passes_doc_id_param(
		self,
		documents_data: DocumentsData,
		mock_conn: object,
	) -> None:
		"""``spxi_get`` sends a GET with ``docId`` query parameter.

		Parameters
		----------
		documents_data : DocumentsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		documents_data.spxi_get(doc_id="D3")
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/document/D3/lmt/spxi",
			dict_params={"docId": "D3"},
		)

	def test_client_infos_calls_get_with_document_id_param(
		self,
		documents_data: DocumentsData,
		mock_conn: object,
	) -> None:
		"""``client_infos`` sends a GET to ``/api/v1.0/account`` using ``documentId``.

		Parameters
		----------
		documents_data : DocumentsData
			Adapter under test.
		mock_conn : object
			Mocked connection.

		Returns
		-------
		None
		"""
		documents_data.client_infos(doc_id="D4")
		mock_conn.app_request.assert_called_once_with(
			method="GET",
			app="/api/v1.0/account",
			dict_params={
				"participantCode": "0001",
				"pnpCode": "1",
				"documentId": "D4",
			},
			bool_retry_if_error=True,
		)
