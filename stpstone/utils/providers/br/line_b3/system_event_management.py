"""B3 LINE API system event management adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from datetime import date, datetime
from typing import Any, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import (
	IConnectionApi,
	ISystemEventManagement,
)


class SystemEventManagement(metaclass=TypeChecker):
	"""Adapter that fulfils the ``ISystemEventManagement`` port for B3 LINE API events.

	Parameters
	----------
	conn : IConnectionApi
		Connection adapter that handles authentication and raw HTTP.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``ISystemEventManagement`` port.
	"""

	def __init__(self, conn: IConnectionApi) -> None:
		"""Initialize SystemEventManagement with an ``IConnectionApi`` dependency.

		Parameters
		----------
		conn : IConnectionApi
			Connection adapter that handles authentication and raw HTTP.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``ISystemEventManagement`` port.
		"""
		self._conn = conn

		if not isinstance(self, ISystemEventManagement):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy ISystemEventManagement — "
				"implement report()."
			)

	def report(
		self,
		date_start: Union[datetime, date],
		date_end: Union[datetime, date],
		str_start_time: str = "00:00",
		str_sup_time: str = "23:59",
		int_entity_type: int = 3,
	) -> Union[list[dict[str, Any]], int]:
		"""Generate system event report.

		Parameters
		----------
		date_start : Union[datetime, date]
			Start date.
		date_end : Union[datetime, date]
			End date.
		str_start_time : str
			Start time (default: ``"00:00"``).
		str_sup_time : str
			End time (default: ``"23:59"``).
		int_entity_type : int
			Entity type (default: 3).

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Report data or status code.
		"""
		dict_payload = {
			"participantCode": int(self._conn.broker_code),
			"categoryType": int(self._conn.category_code),
			"entityType": int_entity_type,
			"carryingAccountCode": "null",
			"pnpCode": "",
			"accountTypeLineDomain": "null",
			"ownerName": "null",
			"documentCode": "null",
			"accountCode": "null",
			"startTime": str_start_time,
			"endTime": str_sup_time,
			"startDate": date_start.strftime("%d/%m/%Y"),
			"endDate": date_end.strftime("%d/%m/%Y"),
		}
		return self._conn.app_request(
			method="POST",
			app="/api/v1.0/systemEvent",
			dict_payload=dict_payload,
			bool_parse_dict_params_data=True,
		)
