"""B3 LINE API professional data adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from datetime import date, datetime
from typing import Any, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi, IProfessional


class Professional(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IProfessional`` port for B3 LINE API professional data.

	Parameters
	----------
	conn : IConnectionApi
		Connection adapter that handles authentication and raw HTTP.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IProfessional`` port.
	"""

	def __init__(self, conn: IConnectionApi) -> None:
		"""Initialize Professional with an ``IConnectionApi`` dependency.

		Parameters
		----------
		conn : IConnectionApi
			Connection adapter that handles authentication and raw HTTP.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IProfessional`` port.
		"""
		self._conn = conn

		if not isinstance(self, IProfessional):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IProfessional — "
				"implement professional_code_get() and professional_historic_position()."
			)

	def professional_code_get(self) -> Union[list[dict[str, Any]], int]:
		"""Retrieve professional information by code.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Professional data or status code.
		"""
		dict_params = {
			"participantCode": self._conn.broker_code,
			"pnpCode": self._conn.category_code,
		}
		return self._conn.app_request(
			method="GET",
			app="/api/v1.0/operationsProfessionalParticipant/code",
			dict_params=dict_params,
		)

	def professional_historic_position(
		self,
		professional_code: str,
		date_start: Union[datetime, date],
		date_end: Union[datetime, date],
		int_participant_perspective_type: int = 0,
		entity_type: int = 4,
	) -> Union[list[dict[str, Any]], int]:
		"""Retrieve professional position history.

		Parameters
		----------
		professional_code : str
			Professional identifier.
		date_start : Union[datetime, date]
			Start date.
		date_end : Union[datetime, date]
			End date.
		int_participant_perspective_type : int
			Participant perspective type, by default 0.
		entity_type : int
			Entity type, by default 4.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Position history data or status code.
		"""
		list_metric_type: list[int] = [1, 2, 3, 4, 6, 7, 22, 25, 26, 27, 28, 29, 36, 38, 39]
		int_items_per_page: int = 50

		dict_payload = {
			"angularItensPerPage": int_items_per_page,
			"entityType": entity_type,
			"metricTypes": list_metric_type,
			"ownerBrokerCode": int(self._conn.broker_code),
			"ownerCategoryType": int(self._conn.category_code),
			"partPerspecType": int_participant_perspective_type,
			"registryDateEnd": date_end.strftime("%Y-%m-%d"),
			"registryDateStart": date_start.strftime("%Y-%m-%d"),
			"traderCode": professional_code,
		}

		return self._conn.app_request(
			method="POST",
			app="https://api.line.trd.cert.bvmfnet.com.br/api/v2.0/position/hstry",
			dict_payload=dict_payload,
			bool_retry_if_error=True,
			bool_parse_dict_params_data=True,
		)
