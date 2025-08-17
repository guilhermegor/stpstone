"""Network information utilities.

This module provides a class for retrieving network-related information such as IP addresses,
port status, and open ports using socket, requests, and psutil libraries.
"""

import socket

import psutil
import requests

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class NetworkInfo(metaclass=TypeChecker):
    """Class for retrieving network-related information."""

    def _validate_port(self, port: int) -> None:
        """Validate port number.

        Parameters
        ----------
        port : int
            Port number to validate

        Raises
        ------
        ValueError
            If port is not between 0 and 65535
        """
        if not 0 <= port <= 65535:
            raise ValueError("Port must be between 0 and 65535")

    def get_public_ip_address(self) -> str:
        """Retrieve the public IP address of the machine using an external service.

        Returns
        -------
        str
            The public IP address
        """
        try:
            response = requests.get("https://api.ipify.org?format=json", timeout=10)
            response.raise_for_status()
            return str(response.json()["ip"])
        except requests.RequestException as err:
            raise requests.RequestException(
                f"Error fetching public IP: {str(err)}"
            ) from err

    def is_port_in_use(self, port: int) -> bool:
        """Check if a specific port is in use on the local machine.

        Parameters
        ----------
        port : int
            The port number to check

        Returns
        -------
        bool
            True if the port is in use, False otherwise
        """
        self._validate_port(port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("0.0.0.0", port)) # noqa S104: possible binding to all interfaces
                return False
            except OSError:
                return True

    def get_available_port(self) -> int:
        """Find and return an available port on the local machine.

        Returns
        -------
        int
            An available port number
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("0.0.0.0", 0)) # noqa S104: possible binding to all interfaces
            return int(sock.getsockname()[1])

    def get_open_ports(self) -> list[int]:
        """Retrieve a list of all open ports on the local machine.

        Returns
        -------
        list[int]
            A list of open port numbers
        """
        open_ports: list[int] = []
        for conn in psutil.net_connections():
            if conn.status == psutil.CONN_LISTEN and conn.laddr:
                open_ports.append(conn.laddr.port)
        return open_ports