import socket
import requests
import psutil


class NetworkInfo:
    """
    A class to retrieve network-related information such as IP addresses, port status, and open ports.
    """

    @property
    def get_public_ip_address(self):
        """
        Retrieves the public IP address of the machine using an external service.
        Returns:
            str: The public IP address, or an error message if the request fails.
        """
        try:
            response = requests.get('https://api.ipify.org?format=json')
            response.raise_for_status()
            public_ip = response.json()['ip']
            return public_ip
        except requests.RequestException as e:
            return f'Error fetching public IP: {e}'

    def is_port_in_use(self, port):
        """
        Checks if a specific port is in use on the local machine.

        Args:
            port (int): The port number to check.

        Returns:
            bool: True if the port is in use, False otherwise.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                #   try to bind to the port
                s.bind(('0.0.0.0', port))
                #   port is available
                return False
            except OSError:
                #   port is in use
                return True

    @property
    def get_available_port(self):
        """
        Finds and returns an available port on the local machine.

        Returns:
            int: An available port number.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #   bind to an available port (0 means OS assigns a free port)
            s.bind(('0.0.0.0', 0))
            #   get the assigned port number
            port = s.getsockname()[1]
            return port

    @property
    def get_open_ports(self):
        """
        Retrieves a list of all open ports on the local machine.

        Returns:
            list: A list of open port numbers.
        """
        open_ports = []
        for conn in psutil.net_connections():
            if conn.status == psutil.CONN_LISTEN and conn.laddr:
                open_ports.append(conn.laddr.port)
        return open_ports


if __name__ == '__main__':
    network_info = NetworkInfo()

    # get public IP address
    public_ip = network_info.get_public_ip_address
    print(f'Public IP Address: {public_ip}')

    # check if a port is in use
    port_to_check = 8080
    if network_info.is_port_in_use(port_to_check):
        print(f'Port {port_to_check} is in use.')
    else:
        print(f'Port {port_to_check} is available.')

    # get an available port
    available_port = network_info.get_available_port
    print(f'Available Port: {available_port}')

    # get all opened ports
    open_ports = network_info.get_open_ports
    print(f'Open Ports: {open_ports}')
