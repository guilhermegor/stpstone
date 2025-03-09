import platform
import subprocess
import psutil
from typing import Dict, List, Optional
import time

class DriveClassifier:
    """
    A utility class for classifying and retrieving information about disk drives.
    """

    @property
    def classify_drives(self) -> Dict[str, List[str]]:
        """
        Classifies hard drive units as 'local' or 'detachable'.

        Returns:
            Dict[str, List[str]]: A dictionary with two keys:
                - 'local': List of local drive paths.
                - 'detachable': List of detachable drive paths.
        """
        dict_drives = {
            "local": [],
            "detachable": []
        }
        psutil_partitions = psutil.disk_partitions()
        for partition in psutil_partitions:
            if 'removable' in partition.opts:
                dict_drives["detachable"].append(partition.device)
            else:
                dict_drives["local"].append(partition.device)
        return dict_drives

    def get_drive_usage(self, drive_path: str) -> Optional[Dict[str, int]]:
        """
        Retrieves usage statistics for a specific drive.

        Args:
            drive_path[str]: The path of the drive (e.g., 'C:\\').

        Returns:
            dict: A dictionary with keys 'total', 'used', and 'free' (in bytes).
                  Returns None if the drive is not found.
        """
        try:
            usage = psutil.disk_usage(drive_path)
            return {
                "total": usage.total,
                "used": usage.used,
                "free": usage.free
            }
        except FileNotFoundError:
            return None

    def get_drive_info(self) -> List[Dict[str, str]]:
        """
        Retrieves detailed information about all drives.

        Returns:
            list: A list of dictionaries, each containing drive information such as
                  device, mount point, file system type, and options.
        """
        psutil_partitions = psutil.disk_partitions()
        drive_info = []
        for partition in psutil_partitions:
            drive_info.append({
                "device": partition.device,
                "mountpoint": partition.mountpoint,
                "fstype": partition.fstype,
                "opts": partition.opts
            })
        return drive_info

    def list_all_drives(self) -> List[str]:
        """
        Lists all drives without classification.

        Returns:
            list: A list of all drive paths.
        """
        psutil_partitions = psutil.disk_partitions()
        return [partition.device for partition in psutil_partitions]

    def filter_drives_by_fs(self, fs_type: str) -> List[str]:
        """
        Filters drives by their file system type.

        Args:
            fs_type[str]: The file system type to filter by (e.g., 'NTFS', 'ext4').

        Returns:
            list: A list of drive paths that match the specified file system type.
        """
        psutil_partitions = psutil.disk_partitions()
        return [partition.device for partition in psutil_partitions if partition.fstype == fs_type]

    def monitor_drive_changes(self, interval: int = 5) -> None:
        """
        Monitors for changes in connected drives (e.g., USB drives being connected or disconnected).

        Args:
            interval (int): The time interval (in seconds) between checks.
        """
        previous_drives = set(self.list_all_drives())
        print("Monitoring drive changes. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(interval)
                current_drives = set(self.list_all_drives())
                if current_drives != previous_drives:
                    added = current_drives - previous_drives
                    removed = previous_drives - current_drives
                    if added:
                        print(f"Drives added: {added}")
                    if removed:
                        print(f"Drives removed: {removed}")
                    previous_drives = current_drives
        except KeyboardInterrupt:
            print("Drive monitoring stopped.")

    def get_drive_serial(self, drive_path: str) -> Optional[str]:
        """
        Retrieves the serial number or unique identifier for a drive (platform-dependent).

        Args:
            drive_path (str): The path of the drive (e.g., 'C:\\' on Windows, '/dev/sda' on Linux).

        Returns:
            str: The serial number or unique identifier of the drive.
                 Returns None if the serial number cannot be retrieved.
        """
        system = platform.system()
        if system == "Windows":
            return self._get_drive_serial_windows(drive_path)
        elif system == "Linux":
            return self._get_drive_serial_linux(drive_path)
        # macOS
        elif system == "Darwin":
            return self._get_drive_serial_mac(drive_path)
        else:
            print(f"Unsupported operating system: {system}")
            return None

    def _get_drive_serial_windows(self, drive_path: str) -> Optional[str]:
        """
        Retrieves the serial number for a drive on Windows.

        Args:
            drive_path (str): The path of the drive (e.g., 'C:\\').

        Returns:
            str: The serial number of the drive.
        """
        try:
            import wmi
            c = wmi.WMI()
            for disk in c.Win32_DiskDrive():
                if disk.DeviceID == drive_path:
                    return disk.SerialNumber.strip()
            return None
        except ImportError:
            print("The 'wmi' library is required for this functionality on Windows.")
            return None

    def _get_drive_serial_linux(self, drive_path: str) -> Optional[str]:
        """
        Retrieves the serial number for a drive on Linux.

        Args:
            drive_path (str): The path of the drive (e.g., '/dev/sda').

        Returns:
            str: The serial number of the drive.
        """
        try:
            # Use `hdparm` or `lsblk` to get the serial number
            result = subprocess.run(
                ["sudo", "hdparm", "-I", drive_path],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "Serial Number:" in line:
                        return line.split(":")[1].strip()
            return None
        except FileNotFoundError:
            print("The 'hdparm' command is required for this functionality on Linux.")
            return None

    def _get_drive_serial_mac(self, drive_path: str) -> Optional[str]:
        """
        Retrieves the serial number for a drive on macOS.

        Args:
            drive_path (str): The path of the drive (e.g., '/dev/disk0').

        Returns:
            str: The serial number of the drive.
        """
        try:
            # Use `diskutil` to get the serial number
            result = subprocess.run(
                ["diskutil", "info", drive_path],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "Device / Media Name:" in line:
                        return line.split(":")[1].strip()
            return None
        except FileNotFoundError:
            print("The 'diskutil' command is required for this functionality on macOS.")
            return None