"""Drive classification and information utilities.

This module provides a class for retrieving, classifying, and monitoring disk drives across
different operating systems. It supports gathering usage statistics, filtering by file system
type, and retrieving serial numbers using platform-specific commands.
"""

from logging import Logger
import platform
import shutil
import subprocess
import time
from typing import Optional, TypedDict

import psutil

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog


class ReturnClassifyDrives(TypedDict):
    """Typed dictionary for classify_drives return.

    Keys
    ----
    local : list[str]
        List of local drive paths
    detachable : list[str]
        List of detachable drive paths
    """

    local: list[str]
    detachable: list[str]


class ReturnGetDriveUsage(TypedDict):
    """Typed dictionary for get_drive_usage return.

    Keys
    ----
    total : int
        Total size in bytes
    used : int
        Used size in bytes
    free : int
        Free size in bytes
    """

    total: int
    used: int
    free: int

class ReturnGetDriveInfo(TypedDict):
    """Typed dictionary for get_drive_info return.

    Keys
    ----
    device : str
        Drive device path
    mountpoint : str
        Mount point path
    fstype : str
        File system type
    opts : str
        Mount options
    """

    device: str
    mountpoint: str
    fstype: str
    opts: str


class DriveClassifier(metaclass=TypeChecker):
    """Class for classifying and retrieving information about disk drives."""

    def __init__(self, logger: Optional[Logger] = None) -> None:
        """Initialize DriveClassifier.

        Parameters
        ----------
        logger : Optional[Logger], optional
            Logger instance for logging, by default None
        
        Returns
        -------
        None
        """
        self.logger = logger

    def _validate_drive_path(self, drive_path: str) -> None:
        """Validate that drive path is a non-empty string.

        Parameters
        ----------
        drive_path : str
            Drive path to validate

        Raises
        ------
        ValueError
            If drive_path is empty
            If drive_path is not a string
        """
        if not drive_path:
            raise ValueError("Drive path cannot be empty")
        if not isinstance(drive_path, str):
            raise ValueError("Drive path must be a string")

    def classify_drives(self) -> ReturnClassifyDrives:
        """Classify hard drive units as 'local' or 'detachable'.

        Returns
        -------
        ReturnClassifyDrives
            Dictionary with keys 'local' and 'detachable' containing drive paths

        References
        ----------
        .. [1] https://psutil.readthedocs.io/en/latest/#psutil.disk_partitions
        """
        dict_drives: ReturnClassifyDrives = {"local": [], "detachable": []}
        psutil_partitions = psutil.disk_partitions()
        for partition in psutil_partitions:
            if "removable" in partition.opts:
                dict_drives["detachable"].append(partition.device)
            else:
                dict_drives["local"].append(partition.device)
        return dict_drives

    def get_drive_usage(self, drive_path: str) -> Optional[ReturnGetDriveUsage]:
        """Retrieve usage statistics for a specific drive.

        Parameters
        ----------
        drive_path : str
            Path of the drive

        Returns
        -------
        Optional[ReturnGetDriveUsage]
            Dictionary with keys 'total', 'used', and 'free' in bytes,
            or None if the drive is not found

        References
        ----------
        .. [1] https://psutil.readthedocs.io/en/latest/#psutil.disk_usage
        """
        self._validate_drive_path(drive_path)
        try:
            usage = psutil.disk_usage(drive_path)
            return {
                "total": usage.total,
                "used": usage.used,
                "free": usage.free,
            }
        except FileNotFoundError:
            return None

    def get_drive_info(self) -> list[ReturnGetDriveInfo]:
        """Retrieve detailed information about all drives.

        Returns
        -------
        list[ReturnGetDriveInfo]
            List of dictionaries with device, mountpoint, fstype, and opts

        References
        ----------
        .. [1] https://psutil.readthedocs.io/en/latest/#psutil.disk_partitions
        """
        psutil_partitions = psutil.disk_partitions()
        drive_info: list[ReturnGetDriveInfo] = []
        for partition in psutil_partitions:
            drive_info.append(
                {
                    "device": partition.device,
                    "mountpoint": partition.mountpoint,
                    "fstype": partition.fstype,
                    "opts": partition.opts,
                }
            )
        return drive_info

    def list_all_drives(self) -> list[str]:
        """List all drives without classification.

        Returns
        -------
        list[str]
            List of all drive paths
        """
        psutil_partitions = psutil.disk_partitions()
        return [partition.device for partition in psutil_partitions]

    def filter_drives_by_fs(self, fs_type: str) -> list[str]:
        """Filter drives by their file system type.

        Parameters
        ----------
        fs_type : str
            File system type to filter by (e.g., 'NTFS', 'ext4')

        Returns
        -------
        list[str]
            List of drive paths that match the specified file system type
        """
        psutil_partitions = psutil.disk_partitions()
        return [
            partition.device
            for partition in psutil_partitions
            if partition.fstype == fs_type
        ]

    def monitor_drive_changes(self, interval: int = 5) -> None:
        """Monitor for changes in connected drives.

        Parameters
        ----------
        interval : int
            Time interval in seconds between checks

        Notes
        -----
        Press Ctrl+C to stop monitoring
        """
        previous_drives = set(self.list_all_drives())
        CreateLog().log_message(
                self.logger, 
                "Monitoring drive changes. Press Ctrl+C to stop.", 
                "info"
        )
        try:
            while True:
                time.sleep(interval)
                current_drives = set(self.list_all_drives())
                if current_drives != previous_drives:
                    added = current_drives - previous_drives
                    removed = previous_drives - current_drives
                    if added:
                        CreateLog().log_message(
                            self.logger, 
                            f"Drives added: {added}", 
                            "info"
                        )
                    if removed:
                        CreateLog().log_message(
                            self.logger, 
                            f"Drives removed: {removed}", 
                            "info"
                        )
                    previous_drives = current_drives
        except KeyboardInterrupt:
            CreateLog().log_message(
                self.logger, 
                "Drive monitoring stopped.", 
                "info"
            )

    def get_drive_serial(self, drive_path: str) -> Optional[str]:
        """Retrieve the serial number or unique identifier for a drive.

        Parameters
        ----------
        drive_path : str
            Drive path

        Returns
        -------
        Optional[str]
            Serial number of the drive or None if retrieval fails
        """
        self._validate_drive_path(drive_path)
        system = platform.system()
        if system == "Windows":
            return self._get_drive_serial_windows(drive_path)
        if system == "Linux":
            return self._get_drive_serial_linux(drive_path)
        if system == "Darwin":
            return self._get_drive_serial_mac(drive_path)
        CreateLog().log_message(
            self.logger, 
            f"Unsupported operating system: {system}", 
            "critical"
        )
        return None

    def _get_drive_serial_windows(self, drive_path: str) -> Optional[str]:
        """Retrieve the serial number for a drive on Windows.

        Parameters
        ----------
        drive_path : str
            Path of the drive

        Returns
        -------
        Optional[str]
            Serial number of the drive
        """
        try:
            import wmi

            c = wmi.WMI()
            for disk in c.Win32_DiskDrive():
                if disk.DeviceID == drive_path:
                    return disk.SerialNumber.strip()
            return None
        except ImportError:
            CreateLog().log_message(
                self.logger, 
                "The 'wmi' library is required for this functionality on Windows.", 
                "warning"
            )
            return None

    def _get_drive_serial_linux(self, drive_path: str) -> Optional[str]:
        """Retrieve the serial number for a drive on Linux.

        Parameters
        ----------
        drive_path : str
            Path of the drive (e.g., '/dev/sda')

        Returns
        -------
        Optional[str]
            Serial number of the drive

        Raises
        ------
        SystemError
            If the 'hdparm' command is not found
        """
        self._validate_drive_path(drive_path)
        
        sudo_cmd = shutil.which("sudo")
        hdparm_cmd = shutil.which("hdparm")
        if not sudo_cmd:
            raise SystemError(
                "The 'sudo' command is required for this functionality on Linux.")
        if not hdparm_cmd:
            raise SystemError(
                "The 'hdparm' command is required for this functionality on Linux.")

        try:
            result = subprocess.run( # noqa S603: execution of untrusted input
                [sudo_cmd, hdparm_cmd, "-I", drive_path],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "Serial Number:" in line:
                        return line.split(":", 1)[1].strip()
            return None
        except FileNotFoundError:
            return None

    def _get_drive_serial_mac(self, drive_path: str) -> Optional[str]:
        """Retrieve the serial number for a drive on macOS.

        Parameters
        ----------
        drive_path : str
            Path of the drive (e.g., '/dev/disk0')

        Returns
        -------
        Optional[str]
            Serial number of the drive

        Raises
        ------
        SystemError
            If the 'diskutil' command is not found
        """
        self._validate_drive_path(drive_path)

        diskutil_cmd = shutil.which("diskutil")
        if not diskutil_cmd:
            raise SystemError(
                "The 'diskutil' command is required for this functionality on macOS.")

        try:
            result = subprocess.run( # noqa S603: execution of untrusted input
                [diskutil_cmd, "info", drive_path],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "Device / Media Name:" in line:
                        return line.split(":", 1)[1].strip()
            return None
        except FileNotFoundError:
            return None
