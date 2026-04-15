"""B3 LINE API market operations adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from typing import Any, Literal, Optional, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi, IOperations


class Operations(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IOperations`` port for B3 LINE API market operations.

	Receives an ``IConnectionApi`` dependency and exposes both business methods
	(exchange limits, authorized markets) and a proxied ``app_request`` so that
	``Resources`` only needs to depend on ``IOperations``.

	Parameters
	----------
	conn : IConnectionApi
		Connection adapter that handles authentication and raw HTTP.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IOperations`` port.
	"""

	def __init__(self, conn: IConnectionApi) -> None:
		"""Initialize Operations with an ``IConnectionApi`` dependency.

		Parameters
		----------
		conn : IConnectionApi
			Connection adapter that handles authentication and raw HTTP.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IOperations`` port.
		"""
		self._conn = conn

		if not isinstance(self, IOperations):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IOperations — "
				"implement app_request(), exchange_limits(), groups_authorized_markets(), "
				"intruments_per_group(), and authorized_markets_instruments()."
			)

	def app_request(
		self,
		method: Literal["GET", "POST", "DELETE"],
		app: str,
		dict_params: Optional[dict[str, Any]] = None,
		dict_payload: Optional[list[dict[str, Any]]] = None,
		bool_parse_dict_params_data: bool = False,
		bool_retry_if_error: bool = False,
		float_secs_sleep: Optional[float] = None,
		timeout: Union[tuple[float, float], float, int] = 10,
	) -> Union[list[dict[str, Any]], int]:
		"""Proxy for the underlying connection's ``app_request``.

		Parameters
		----------
		method : Literal['GET', 'POST', 'DELETE']
			HTTP method.
		app : str
			API endpoint.
		dict_params : Optional[dict[str, Any]], optional
			Request parameters (default: None).
		dict_payload : Optional[list[dict[str, Any]]], optional
			Request payload (default: None).
		bool_parse_dict_params_data : bool
			Parse parameters as JSON (default: False).
		bool_retry_if_error : bool
			Enable retry on error (default: False).
		float_secs_sleep : Optional[float]
			Sleep time between retries (default: None).
		timeout : Union[tuple[float, float], float, int], optional
			Request timeout (default: 10).

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Response data or status code.
		"""
		return self._conn.app_request(
			method=method,
			app=app,
			dict_params=dict_params,
			dict_payload=dict_payload,
			bool_parse_dict_params_data=bool_parse_dict_params_data,
			bool_retry_if_error=bool_retry_if_error,
			float_secs_sleep=float_secs_sleep,
			timeout=timeout,
		)

	def exchange_limits(self) -> Union[list[dict[str, Any]], int]:
		"""Retrieve exchange limits for the broker.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Exchange limits data or status code.
		"""
		return self._conn.app_request(
			method="GET",
			app=f"/api/v1.0/exchangeLimits/spxi/{self._conn.broker_code}",
			bool_retry_if_error=True,
		)

	def groups_authorized_markets(self) -> Union[list[dict[str, Any]], int]:
		"""Retrieve authorized market groups.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Market groups data or status code.
		"""
		return self._conn.app_request(
			method="GET",
			app="/api/v1.0/exchangeLimits/autorizedMarkets",
			bool_retry_if_error=True,
		)

	def intruments_per_group(self, group_id: str) -> Union[list[dict[str, Any]], int]:
		"""Retrieve instruments associated with a market group.

		Parameters
		----------
		group_id : str
			Market group identifier.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Instruments data or status code.
		"""
		dict_payload = {
			"authorizedMarketGroupId": group_id,
			"isLimitSetted": "true",
		}
		return self._conn.app_request(
			method="POST",
			app="/api/v1.0/exchangeLimits/findInstruments",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
		)

	def authorized_markets_instruments(self) -> dict[str, Union[str, int, float]]:
		"""Retrieve all authorized market instruments with limits.

		Returns
		-------
		dict[str, Union[str, int, float]]
			Dictionary mapping market groups to their instruments and limits.
		"""
		dict_export: dict[str, Union[str, int, float]] = {}
		json_authorized_markets: Union[list[dict[str, Any]], int] = (
			self.groups_authorized_markets()
		)

		for dict_ in json_authorized_markets:
			for dict_assets in self.intruments_per_group(dict_["id"]):
				if dict_["id"] not in dict_export:
					dict_export[dict_["id"]] = {}

				dict_export[dict_["id"]]["id"] = dict_["id"]
				dict_export[dict_["id"]]["name"] = dict_["name"]

				if "assets_associated" not in dict_export[dict_["id"]]:
					dict_export[dict_["id"]]["assets_associated"] = []

				asset_data = {
					"instrument_symbol": dict_assets["instrumentSymbol"],
					"instrument_asset": dict_assets["instrumentAsset"],
					"limit_spci": dict_assets["limitSpci"],
					"limit_spvi": dict_assets["limitSpvi"],
				}

				if "limitSpciOption" in dict_assets:
					asset_data.update(
						{
							"limit_spci_option": dict_assets["limitSpciOption"],
							"limit_spvi_option": dict_assets["limitSpviOption"],
						}
					)

				dict_export[dict_["id"]]["assets_associated"].append(asset_data)

		return dict_export
