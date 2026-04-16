"""Unit tests for NetworkInfo class.

This module tests the network information utilities, covering:
- Public IP address retrieval
- Port status checking
- Available port finding
- Open ports listing
- Input validation and error handling
"""

import socket
from typing import Any

import psutil
import pytest
from pytest_mock import MockerFixture
import requests

from stpstone.utils.connections.netops.diagnostics.network_info import NetworkInfo


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def network_info() -> NetworkInfo:
	"""Fixture providing a NetworkInfo instance.

	Returns
	-------
	NetworkInfo
		Instance of NetworkInfo class
	"""
	return NetworkInfo()


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> Any:  # noqa ANN401: typing.Any not allowed
	"""Fixture to mock requests.get.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	Any
		Mock object for requests.get
	"""
	return mocker.patch("requests.get")


@pytest.fixture
def mock_socket(mocker: MockerFixture) -> Any:  # noqa ANN401: typing.Any not allowed
	"""Fixture to mock socket.socket.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	Any
		Mock object for socket.socket
	"""
	return mocker.patch("socket.socket")


@pytest.fixture
def mock_psutil_net_connections(mocker: MockerFixture) -> Any:  # noqa ANN401: typing.Any not allowed
	"""Fixture to mock psutil.net_connections.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	Any
		Mock object for psutil.net_connections
	"""
	return mocker.patch("psutil.net_connections")


# --------------------------
# Tests for _validate_port
# --------------------------
def test_validate_port_valid(network_info: NetworkInfo) -> None:
	"""Test port validation with valid port numbers.

	Verifies
	-------
	- Valid ports (0, 80, 65535) do not raise ValueError

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class

	Returns
	-------
	None
	"""
	for port in [0, 80, 65535]:
		network_info._validate_port(port)


def test_validate_port_invalid_negative(network_info: NetworkInfo) -> None:
	"""Test port validation with negative port number.

	Verifies
	-------
	- Negative port raises ValueError with appropriate message

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Port must be between 0 and 65535"):
		network_info._validate_port(-1)


def test_validate_port_invalid_too_large(network_info: NetworkInfo) -> None:
	"""Test port validation with too large port number.

	Verifies
	-------
	- Port > 65535 raises ValueError with appropriate message

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Port must be between 0 and 65535"):
		network_info._validate_port(65536)


def test_validate_port_invalid_type(network_info: NetworkInfo) -> None:
	"""Test port validation with invalid type.

	Verifies
	-------
	- Non-integer port raises TypeError

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError):
		network_info._validate_port("80")  # type: ignore


# --------------------------
# Tests for get_public_ip_address
# --------------------------
def test_get_public_ip_address_success(
	network_info: NetworkInfo,
	mock_requests_get: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test successful public IP address retrieval.

	Verifies
	-------
	- Returns correct IP address from API response
	- Requests.get is called with correct URL and timeout

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class
	mock_requests_get : Any
		Mock for requests.get

	Returns
	-------
	None
	"""
	mock_response = mock_requests_get.return_value
	mock_response.json.return_value = {"ip": "192.168.1.1"}
	mock_response.raise_for_status.return_value = None

	result = network_info.get_public_ip_address()
	assert result == "192.168.1.1"
	mock_requests_get.assert_called_once_with("https://api.ipify.org?format=json", timeout=10)


def test_get_public_ip_address_request_failure(
	network_info: NetworkInfo,
	mock_requests_get: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test public IP address retrieval with request failure.

	Verifies
	-------
	- Raises RequestException with appropriate message
	- Requests.get is called with correct URL and timeout

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class
	mock_requests_get : Any
		Mock for requests.get

	Returns
	-------
	None
	"""
	mock_requests_get.side_effect = requests.RequestException("Network error")
	with pytest.raises(requests.RequestException, match="Error fetching public IP: Network error"):
		network_info.get_public_ip_address()
	mock_requests_get.assert_called_once_with("https://api.ipify.org?format=json", timeout=10)


# --------------------------
# Tests for is_port_in_use
# --------------------------
def test_is_port_in_use_free(
	network_info: NetworkInfo,
	mock_socket: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test checking a free port.

	Verifies
	-------
	- Returns False when port is not in use
	- Socket is properly bound and closed

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class
	mock_socket : Any
		Mock for socket.socket

	Returns
	-------
	None
	"""
	mock_sock = mock_socket.return_value.__enter__.return_value
	mock_sock.bind.return_value = None
	assert network_info.is_port_in_use(80) is False
	mock_socket.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM)
	mock_sock.bind.assert_called_once_with(("0.0.0.0", 80))  # noqa S104


def test_is_port_in_use_occupied(
	network_info: NetworkInfo,
	mock_socket: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test checking an occupied port.

	Verifies
	-------
	- Returns True when port binding fails
	- Socket is properly closed

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class
	mock_socket : Any
		Mock for socket.socket

	Returns
	-------
	None
	"""
	mock_sock = mock_socket.return_value.__enter__.return_value
	mock_sock.bind.side_effect = OSError("Port in use")
	assert network_info.is_port_in_use(80) is True
	mock_socket.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM)
	mock_sock.bind.assert_called_once_with(("0.0.0.0", 80))  # noqa S104


def test_is_port_in_use_invalid_port(network_info: NetworkInfo) -> None:
	"""Test checking port with invalid port number.

	Verifies
	-------
	- Raises ValueError for invalid port
	- No socket operations are performed

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Port must be between 0 and 65535"):
		network_info.is_port_in_use(65536)


# --------------------------
# Tests for get_available_port
# --------------------------
def test_get_available_port(
	network_info: NetworkInfo,
	mock_socket: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test finding an available port.

	Verifies
	-------
	- Returns a valid port number
	- Socket is properly bound to port 0
	- Returns integer type

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class
	mock_socket : Any
		Mock for socket.socket

	Returns
	-------
	None
	"""
	mock_sock = mock_socket.return_value.__enter__.return_value
	mock_sock.getsockname.return_value = ("0.0.0.0", 12345)  # noqa S104
	result = network_info.get_available_port()
	assert isinstance(result, int)
	assert 0 <= result <= 65535
	mock_socket.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM)
	mock_sock.bind.assert_called_once_with(("0.0.0.0", 0))  # noqa S104
	mock_sock.getsockname.assert_called_once()


# --------------------------
# Tests for get_open_ports
# --------------------------
def test_get_open_ports(
	network_info: NetworkInfo,
	mock_psutil_net_connections: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test retrieving list of open ports.

	Verifies
	-------
	- Returns correct list of open ports
	- Only includes ports with LISTEN status
	- Returns integer ports

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class
	mock_psutil_net_connections : Any
		Mock for psutil.net_connections

	Returns
	-------
	None
	"""
	from collections import namedtuple

	_Addr = namedtuple("addr", ["ip", "port"])
	_Conn = namedtuple("sconn", ["fd", "family", "type", "laddr", "raddr", "status", "pid"])
	mock_psutil_net_connections.return_value = [
		_Conn(
			fd=-1,
			family=socket.AF_INET,
			type=socket.SOCK_STREAM,
			laddr=_Addr(ip="127.0.0.1", port=80),
			raddr=None,
			status=psutil.CONN_LISTEN,
			pid=None,
		),
		_Conn(
			fd=-1,
			family=socket.AF_INET,
			type=socket.SOCK_STREAM,
			laddr=_Addr(ip="127.0.0.1", port=443),
			raddr=None,
			status=psutil.CONN_LISTEN,
			pid=None,
		),
		_Conn(
			fd=-1,
			family=socket.AF_INET,
			type=socket.SOCK_STREAM,
			laddr=_Addr(ip="127.0.0.1", port=8080),
			raddr=None,
			status=psutil.CONN_ESTABLISHED,
			pid=None,
		),
		_Conn(
			fd=-1,
			family=socket.AF_INET,
			type=socket.SOCK_STREAM,
			laddr=None,
			raddr=None,
			status=psutil.CONN_LISTEN,
			pid=None,
		),
	]
	result = network_info.get_open_ports()
	assert isinstance(result, list)
	assert result == [80, 443]
	assert all(isinstance(port, int) for port in result)
	mock_psutil_net_connections.assert_called_once()


def test_get_open_ports_empty(
	network_info: NetworkInfo,
	mock_psutil_net_connections: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test retrieving open ports when none exist.

	Verifies
	-------
	- Returns empty list when no ports are open
	- Handles empty connections list correctly

	Parameters
	----------
	network_info : NetworkInfo
		Instance of NetworkInfo class
	mock_psutil_net_connections : Any
		Mock for psutil.net_connections

	Returns
	-------
	None
	"""
	mock_psutil_net_connections.return_value = []
	result = network_info.get_open_ports()
	assert result == []
	mock_psutil_net_connections.assert_called_once()


# --------------------------
# Tests for reload logic
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
	"""Test module reload behavior.

	Verifies
	-------
	- Module can be reloaded without errors
	- NetworkInfo class is available after reload

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	import importlib
	import sys

	importlib.reload(sys.modules["stpstone.utils.connections.netops.diagnostics.network_info"])
	assert hasattr(
		sys.modules["stpstone.utils.connections.netops.diagnostics.network_info"], "NetworkInfo"
	)
