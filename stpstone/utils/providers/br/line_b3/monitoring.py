"""B3 LINE API monitoring adapter.

References
----------
.. [1] http://www.b3.com.br/data/files/2E/95/28/F1/EBD17610515A8076AC094EA8/GUIDE-TO-LINE-5.0-API.pdf
.. [2] https://line.bvmfnet.com.br/#/endpoints
"""

from typing import Any, Union

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.providers.br.line_b3._ports import IConnectionApi, IMonitoring


class Monitoring(metaclass=TypeChecker):
	"""Adapter that fulfils the ``IMonitoring`` port for B3 LINE API monitoring alerts.

	Parameters
	----------
	conn : IConnectionApi
		Connection adapter that handles authentication and raw HTTP.

	Raises
	------
	NotImplementedError
		If this class does not satisfy the ``IMonitoring`` port.
	"""

	def __init__(self, conn: IConnectionApi) -> None:
		"""Initialize Monitoring with an ``IConnectionApi`` dependency.

		Parameters
		----------
		conn : IConnectionApi
			Connection adapter that handles authentication and raw HTTP.

		Raises
		------
		NotImplementedError
			If this class does not satisfy the ``IMonitoring`` port.
		"""
		self._conn = conn

		if not isinstance(self, IMonitoring):
			raise NotImplementedError(
				f"{type(self).__name__} does not satisfy IMonitoring — implement alerts()."
			)

	def alerts(self) -> Union[list[dict[str, Any]], int]:
		"""Retrieve latest monitoring alerts.

		Returns
		-------
		Union[list[dict[str, Any]], int]
			Alert data or status code.
		"""
		return self._conn.app_request(
			method="GET",
			app="/api/v1.0/alert/lastalerts?filterRead=true",
			bool_retry_if_error=True,
		)
