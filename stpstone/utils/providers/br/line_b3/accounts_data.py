"""B3 LINE API account data adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from typing import Any, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IAccountsData, IConnectionApi


class AccountsData(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IAccountsData`` port for B3 LINE API account data.

	Parameters
	----------
	conn : IConnectionApi
		Connection adapter that handles authentication and raw HTTP.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IAccountsData`` port.
	"""

	def __init__(self, conn: IConnectionApi) -> None:
		"""Initialize AccountsData with an ``IConnectionApi`` dependency.

		Parameters
		----------
		conn : IConnectionApi
			Connection adapter that handles authentication and raw HTTP.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IAccountsData`` port.
		"""
		self._conn = conn

		if not isinstance(self, IAccountsData):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IAccountsData — "
				"implement all required account methods."
			)

	def client_infos(self, account_code: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve client account information.

		Parameters
		----------
		account_code : str
			Account identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Account data or status code.
		"""
		dict_params = {
			"participantCode": self._conn.broker_code,
			"pnpCode": self._conn.category_code,
			"accountCode": account_code,
		}
		return self._conn.app_request(
			method="GET",
			app="/api/v1.0/account",
			dict_params=dict_params,
			bool_retry_if_error=True,
		)

	def spxi_get(self, account_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve SPCI/SPVI limits for an account.

		Parameters
		----------
		account_id : str
			Account identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Limits data or status code.
		"""
		dict_params = {"accId": account_id}
		return self._conn.app_request(
			method="GET",
			app=f"/api/v1.0/account/{account_id}/lmt/spxi",
			dict_params=dict_params,
		)

	def spxi_instrument_post(
		self,
		account_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Set SPCI/SPVI limits for account instruments.

		Parameters
		----------
		account_id : str
			Account identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			List of instrument limit dictionaries.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/account/{account_id}/lmt/spxi",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
		)

	def spxi_instrument_delete(
		self,
		account_id: str,
		dict_payload: Optional[list[dict[str, Any]]],
	) -> Union[list[dict[str, Any]], int]:
		"""Remove SPCI/SPVI limits for account instruments.

		Parameters
		----------
		account_id : str
			Account identifier.
		dict_payload : Optional[list[dict[str, Any]]]
			List of instrument removal dictionaries.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		return self._conn.app_request(
			method="POST",
			app=f"/api/v1.0/account/{account_id}/lmt/spxi",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
		)

	def spxi_tmox_global_metrics_remove(self, account_id: str) -> Union[list[dict[str, Any]], int]:
		"""Remove all global metrics limits for an account.

		Parameters
		----------
		account_id : str
			Account identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		dict_params = {"accId": account_id}
		return self._conn.app_request(
			method="DELETE",
			app=f"/api/v1.0/account/{account_id}/lmt",
			dict_params=dict_params,
		)

	def specific_global_metric_remotion(
		self,
		account_id: str,
		metric: str,
	) -> Union[list[dict[str, Any]], int]:
		"""Remove specific global metric limit for an account.

		Parameters
		----------
		account_id : str
			Account identifier.
		metric : str
			Metric identifier to remove.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Result data or status code.
		"""
		dict_params = {"accId": account_id, "metric": metric}
		return self._conn.app_request(
			method="DELETE",
			app=f"/api/v2.0/account/{account_id}/lmt",
			dict_params=dict_params,
		)
