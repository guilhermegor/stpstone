"""B3 LINE API document data adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from typing import Any, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi, IDocumentsData


class DocumentsData(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IDocumentsData`` port for B3 LINE API document data.

	Parameters
	----------
	conn : IConnectionApi
		Connection adapter that handles authentication and raw HTTP.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IDocumentsData`` port.
	"""

	def __init__(self, conn: IConnectionApi) -> None:
		"""Initialize DocumentsData with an ``IConnectionApi`` dependency.

		Parameters
		----------
		conn : IConnectionApi
			Connection adapter that handles authentication and raw HTTP.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IDocumentsData`` port.
		"""
		self._conn = conn

		if not isinstance(self, IDocumentsData):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IDocumentsData — "
				"implement all required document methods."
			)

	def doc_info(self, doc_code: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve document information by code.

		Parameters
		----------
		doc_code : str
			Document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Document data or status code.
		"""
		dict_params = {
			"participantCode": self._conn.broker_code,
			"pnpCode": self._conn.category_code,
			"documentCode": str(doc_code),
		}
		return self._conn.app_request(
			method="GET",
			app="/api/v1.0/document",
			dict_params=dict_params,
			bool_retry_if_error=True,
		)

	def block_unblock_doc(self, doc_id: str) -> Union[list[dict[str, Any]], int]:
		"""Block or unblock a document.

		Parameters
		----------
		doc_id : str
			Document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		dict_params = {"id": str(doc_id), "isBlocked": True}
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/document/{doc_id}",
			dict_params=dict_params,
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)

	def update_profile(
		self,
		doc_id: str,
		doc_profile_id: str,
		int_rmkt_evaluation: int = 0,
	) -> Union[list[dict[str, Any]], int]:
		"""Update document risk profile.

		Parameters
		----------
		doc_id : str
			Document identifier.
		doc_profile_id : str
			New profile identifier.
		int_rmkt_evaluation : int, optional
			Risk market evaluation (default: 0).

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		dict_payload = {
			"id": str(doc_id),
			"profileFull": int(doc_profile_id),
			"rmktEvaluation": int_rmkt_evaluation,
		}
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/document/{str(doc_id)}",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)

	def is_protection_mode(self, doc_id: str) -> Union[list[dict[str, Any]], int]:
		"""Set document protection mode.

		Parameters
		----------
		doc_id : str
			Document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		dict_payload = {
			"id": str(doc_id),
			"isProtected": "true",
		}
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/document/{doc_id}",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)

	def client_infos(self, doc_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve client information by document ID.

		Parameters
		----------
		doc_id : str
			Document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Client data or status code.
		"""
		dict_params = {
			"participantCode": self._conn.broker_code,
			"pnpCode": self._conn.category_code,
			"documentId": doc_id,
		}
		return self._conn.app_request(
			method="GET",
			app="/api/v1.0/account",
			dict_params=dict_params,
			bool_retry_if_error=True,
		)

	def doc_profile(self, doc_id: str) -> dict[str, Union[str, int]]:
		"""Retrieve document profile information.

		Parameters
		----------
		doc_id : str
			Document identifier.

		Returns
		-------
		dict[str, Union[str, int]]
			Profile ID and name.
		"""
		json_doc = self._conn.app_request(
			method="GET",
			app=f"/api/v2.0/document/v2.0/document/{doc_id}",
			bool_retry_if_error=True,
		)
		return {
			"profile_id": json_doc["profileFull"],
			"profile_name": json_doc["profileName"],
		}

	def spxi_get(self, doc_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve SPCI/SPVI limits for a document.

		Parameters
		----------
		doc_id : str
			Document identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Limits data or status code.
		"""
		dict_params = {"docId": doc_id}
		return self._conn.app_request(
			method="GET",
			app=f"/api/v1.0/document/{doc_id}/lmt/spxi",
			dict_params=dict_params,
		)

	def spxi_instrument_post(
		self,
		doc_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Set SPCI/SPVI limits for document instruments.

		Parameters
		----------
		doc_id : str
			Document identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			List of instrument limit dictionaries.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/document/{doc_id}/lmt/spxi",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
			bool_retry_if_error=True,
		)

	def spxi_instrument_delete(
		self,
		doc_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Remove SPCI/SPVI limits for document instruments.

		Parameters
		----------
		doc_id : str
			Document identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			List of instrument removal dictionaries.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/document/{doc_id}/lmt/spxi",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
		)
