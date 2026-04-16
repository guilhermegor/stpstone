"""Unit tests for DriveClassifier class.

Tests the drive classification and information retrieval functionality including:
- Drive classification into local and detachable
- Drive usage statistics retrieval
- Drive information gathering
- Drive filtering and monitoring
- Serial number retrieval across different operating systems
- Edge cases and error conditions
- Type validation and fallback mechanisms
"""

from logging import Logger
from unittest.mock import Mock, patch

import pytest

from stpstone.utils.system.drives import DriveClassifier


# --------------------------
# Mock Data
# --------------------------
def create_mock_partition(
	device: str = "/dev/sda1",
	mountpoint: str = "/",
	fstype: str = "ext4",
	opts: str = "rw,relatime",
) -> Mock:
	"""Create a mock partition object.

	Parameters
	----------
	device : str
		Device path, by default "/dev/sda1"
	mountpoint : str
		Mount point, by default "/"
	fstype : str
		File system type, by default "ext4"
	opts : str
		Mount options, by default "rw,relatime"

	Returns
	-------
	Mock
		Mock partition object with specified attributes
	"""
	mock_partition = Mock()
	mock_partition.device = device
	mock_partition.mountpoint = mountpoint
	mock_partition.fstype = fstype
	mock_partition.opts = opts
	return mock_partition


def create_mock_disk_usage(
	total: int = 1000000000,
	used: int = 500000000,
	free: int = 500000000,
) -> Mock:
	"""Create a mock disk usage object.

	Parameters
	----------
	total : int
		Total disk space in bytes, by default 1000000000
	used : int
		Used disk space in bytes, by default 500000000
	free : int
		Free disk space in bytes, by default 500000000

	Returns
	-------
	Mock
		Mock disk usage object with specified attributes
	"""
	mock_usage = Mock()
	mock_usage.total = total
	mock_usage.used = used
	mock_usage.free = free
	return mock_usage


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Mock:
	"""Fixture providing a mock logger instance.

	Returns
	-------
	Mock
		Mock logger object
	"""
	return Mock(spec=Logger)


@pytest.fixture
def drive_classifier() -> DriveClassifier:
	"""Fixture providing DriveClassifier instance without logger.

	Returns
	-------
	DriveClassifier
		DriveClassifier instance with no logger
	"""
	return DriveClassifier()


@pytest.fixture
def drive_classifier_with_logger(mock_logger: Mock) -> DriveClassifier:
	"""Fixture providing DriveClassifier instance with mock logger.

	Parameters
	----------
	mock_logger : Mock
		Mock logger from fixture

	Returns
	-------
	DriveClassifier
		DriveClassifier instance with mock logger
	"""
	return DriveClassifier(logger=mock_logger)


@pytest.fixture
def mock_partitions_mixed() -> list[Mock]:
	"""Fixture providing mixed local and removable partitions.

	Returns
	-------
	list[Mock]
		List of mock partition objects with mixed types
	"""
	return [
		create_mock_partition("/dev/sda1", "/", "ext4", "rw,relatime"),
		create_mock_partition("/dev/sdb1", "/media/usb", "vfat", "rw,removable"),
		create_mock_partition("C:\\", "C:\\", "NTFS", "rw"),
		create_mock_partition("D:\\", "D:\\", "FAT32", "rw,removable"),
	]


@pytest.fixture
def mock_partitions_local_only() -> list[Mock]:
	"""Fixture providing only local partitions.

	Returns
	-------
	list[Mock]
		List of mock partition objects for local drives only
	"""
	return [
		create_mock_partition("/dev/sda1", "/", "ext4", "rw,relatime"),
		create_mock_partition("C:\\", "C:\\", "NTFS", "rw"),
	]


@pytest.fixture
def mock_partitions_empty() -> list[Mock]:
	"""Fixture providing empty partitions list.

	Returns
	-------
	list[Mock]
		Empty list of partitions
	"""
	return []


@pytest.fixture
def sample_drive_usage() -> Mock:
	"""Fixture providing sample drive usage data.

	Returns
	-------
	Mock
		Mock disk usage object with sample data
	"""
	return create_mock_disk_usage(
		total=2000000000,
		used=800000000,
		free=1200000000,
	)


# --------------------------
# Tests - Initialization
# --------------------------
def test_init_without_logger() -> None:
	"""Test initialization without logger parameter.

	Verifies
	--------
	- DriveClassifier can be initialized without logger
	- Logger attribute is None when not provided
	- Instance is properly created

	Returns
	-------
	None
	"""
	classifier = DriveClassifier()
	assert classifier.logger is None
	assert isinstance(classifier, DriveClassifier)


def test_init_with_logger(mock_logger: Mock) -> None:
	"""Test initialization with logger parameter.

	Verifies
	--------
	- DriveClassifier can be initialized with logger
	- Logger attribute is correctly set
	- Instance is properly created

	Parameters
	----------
	mock_logger : Mock
		Mock logger from fixture

	Returns
	-------
	None
	"""
	classifier = DriveClassifier(logger=mock_logger)
	assert classifier.logger is mock_logger
	assert isinstance(classifier, DriveClassifier)


def test_init_with_none_logger() -> None:
	"""Test initialization with explicit None logger.

	Verifies
	--------
	- DriveClassifier accepts None as logger parameter
	- Logger attribute is None when explicitly set to None
	- Instance is properly created

	Returns
	-------
	None
	"""
	classifier = DriveClassifier(logger=None)
	assert classifier.logger is None
	assert isinstance(classifier, DriveClassifier)


# --------------------------
# Tests - Drive Path Validation
# --------------------------
def test_validate_drive_path_valid_string(drive_classifier: DriveClassifier) -> None:
	"""Test _validate_drive_path with valid string input.

	Verifies
	--------
	- Valid non-empty string passes validation
	- No exception is raised for valid input
	- Method completes successfully

	Parameters
	----------
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	# should not raise any exception
	drive_classifier._validate_drive_path("/dev/sda1")
	drive_classifier._validate_drive_path("C:\\")
	drive_classifier._validate_drive_path("/")


def test_validate_drive_path_empty_string(drive_classifier: DriveClassifier) -> None:
	"""Test _validate_drive_path with empty string.

	Verifies
	--------
	- Empty string raises ValueError
	- Error message mentions empty path
	- Validation correctly identifies invalid input

	Parameters
	----------
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Drive path cannot be empty"):
		drive_classifier._validate_drive_path("")


def test_validate_drive_path_whitespace_only(
	drive_classifier: DriveClassifier,
) -> None:
	"""Test _validate_drive_path with whitespace-only string.

	Verifies
	--------
	- Whitespace-only string is considered valid (truthy)
	- No exception is raised for whitespace string
	- Method accepts string with spaces

	Parameters
	----------
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	# whitespace string is truthy, so should pass
	drive_classifier._validate_drive_path("   ")
	drive_classifier._validate_drive_path("\t")
	drive_classifier._validate_drive_path("\n")


def test_validate_drive_path_non_string_types(
	drive_classifier: DriveClassifier,
) -> None:
	"""Test _validate_drive_path with non-string types.

	Verifies
	--------
	- Non-string types raise ValueError
	- Error message mentions string requirement
	- Various non-string types are properly rejected

	Parameters
	----------
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		drive_classifier._validate_drive_path(123)

	with pytest.raises(TypeError, match="must be of type"):
		drive_classifier._validate_drive_path(None)

	with pytest.raises(TypeError, match="must be of type"):
		drive_classifier._validate_drive_path(["/dev/sda1"])

	with pytest.raises(TypeError, match="must be of type"):
		drive_classifier._validate_drive_path({"path": "/dev/sda1"})


# --------------------------
# Tests - Classify Drives
# --------------------------
@patch("psutil.disk_partitions")
def test_classify_drives_mixed_types(
	mock_disk_partitions: Mock,
	mock_partitions_mixed: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test classify_drives with mixed local and removable drives.

	Verifies
	--------
	- Mixed partitions are correctly classified
	- Local drives are placed in 'local' list
	- Removable drives are placed in 'detachable' list
	- Return type matches ReturnClassifyDrives

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_mixed : list[Mock]
		Mixed partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_mixed

	result = drive_classifier.classify_drives()

	assert isinstance(result, dict)
	assert set(result.keys()) == {"local", "detachable"}
	assert "/dev/sda1" in result["local"]
	assert "C:\\" in result["local"]
	assert "/dev/sdb1" in result["detachable"]
	assert "D:\\" in result["detachable"]


@patch("psutil.disk_partitions")
def test_classify_drives_local_only(
	mock_disk_partitions: Mock,
	mock_partitions_local_only: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test classify_drives with only local drives.

	Verifies
	--------
	- All partitions are classified as local
	- Detachable list is empty
	- Local list contains all drive paths
	- Return structure is correct

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_local_only : list[Mock]
		Local-only partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_local_only

	result = drive_classifier.classify_drives()

	assert isinstance(result, dict)
	assert len(result["local"]) == 2
	assert len(result["detachable"]) == 0
	assert "/dev/sda1" in result["local"]
	assert "C:\\" in result["local"]


@patch("psutil.disk_partitions")
def test_classify_drives_empty_partitions(
	mock_disk_partitions: Mock,
	mock_partitions_empty: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test classify_drives with no partitions.

	Verifies
	--------
	- Empty partition list results in empty classifications
	- Both local and detachable lists are empty
	- Method handles empty input gracefully
	- Return structure is correct

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_empty : list[Mock]
		Empty partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_empty

	result = drive_classifier.classify_drives()

	assert isinstance(result, dict)
	assert len(result["local"]) == 0
	assert len(result["detachable"]) == 0


@patch("psutil.disk_partitions")
def test_classify_drives_complex_opts(
	mock_disk_partitions: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test classify_drives with complex mount options.

	Verifies
	--------
	- Drives with 'removable' anywhere in opts are classified as detachable
	- Complex option strings are handled correctly
	- Substring matching for 'removable' works properly

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	complex_partitions = [
		create_mock_partition("/dev/sda1", "/", "ext4", "rw,relatime,removable,sync"),
		create_mock_partition("/dev/sdb1", "/home", "ext4", "rw,relatime"),
		create_mock_partition("/dev/sdc1", "/media", "vfat", "rw,removable"),
	]
	mock_disk_partitions.return_value = complex_partitions

	result = drive_classifier.classify_drives()

	assert "/dev/sda1" in result["detachable"]
	assert "/dev/sdc1" in result["detachable"]
	assert "/dev/sdb1" in result["local"]


# --------------------------
# Tests - Get Drive Usage
# --------------------------
@patch("psutil.disk_usage")
def test_get_drive_usage_valid_path(
	mock_disk_usage: Mock,
	sample_drive_usage: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test get_drive_usage with valid drive path.

	Verifies
	--------
	- Valid drive path returns usage statistics
	- All required keys are present in result
	- Values match expected types and values
	- Method handles successful case correctly

	Parameters
	----------
	mock_disk_usage : Mock
		Mock for psutil.disk_usage
	sample_drive_usage : Mock
		Sample drive usage fixture
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_usage.return_value = sample_drive_usage

	result = drive_classifier.get_drive_usage("/dev/sda1")

	assert result is not None
	assert isinstance(result, dict)
	assert set(result.keys()) == {"total", "used", "free"}
	assert result["total"] == 2000000000
	assert result["used"] == 800000000
	assert result["free"] == 1200000000
	assert all(isinstance(value, int) for value in result.values())


@patch("psutil.disk_usage")
def test_get_drive_usage_nonexistent_path(
	mock_disk_usage: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test get_drive_usage with nonexistent drive path.

	Verifies
	--------
	- Nonexistent drive path returns None
	- FileNotFoundError is caught and handled
	- Method gracefully handles missing drives

	Parameters
	----------
	mock_disk_usage : Mock
		Mock for psutil.disk_usage
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_usage.side_effect = FileNotFoundError("No such device")

	result = drive_classifier.get_drive_usage("/nonexistent/path")

	assert result is None


def test_get_drive_usage_empty_path(drive_classifier: DriveClassifier) -> None:
	"""Test get_drive_usage with empty path.

	Verifies
	--------
	- Empty path raises ValueError
	- Validation is performed before psutil call
	- Error message is appropriate

	Parameters
	----------
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Drive path cannot be empty"):
		drive_classifier.get_drive_usage("")


def test_get_drive_usage_invalid_type(drive_classifier: DriveClassifier) -> None:
	"""Test get_drive_usage with invalid type.

	Verifies
	--------
	- Non-string input raises ValueError
	- Type validation works correctly
	- Error message mentions string requirement

	Parameters
	----------
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		drive_classifier.get_drive_usage(123)


@patch("psutil.disk_usage")
def test_get_drive_usage_zero_values(
	mock_disk_usage: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test get_drive_usage with zero disk usage values.

	Verifies
	--------
	- Zero values are handled correctly
	- Edge case of empty or special drives
	- Return type and structure are maintained

	Parameters
	----------
	mock_disk_usage : Mock
		Mock for psutil.disk_usage
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	zero_usage = create_mock_disk_usage(total=0, used=0, free=0)
	mock_disk_usage.return_value = zero_usage

	result = drive_classifier.get_drive_usage("/dev/empty")

	assert result is not None
	assert result["total"] == 0
	assert result["used"] == 0
	assert result["free"] == 0


@patch("psutil.disk_usage")
def test_get_drive_usage_large_values(
	mock_disk_usage: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test get_drive_usage with large disk usage values.

	Verifies
	--------
	- Large values (TB range) are handled correctly
	- No overflow or precision issues
	- Integer values are preserved

	Parameters
	----------
	mock_disk_usage : Mock
		Mock for psutil.disk_usage
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	large_usage = create_mock_disk_usage(
		total=10**15,  # 1PB
		used=5 * 10**14,  # 500TB
		free=5 * 10**14,  # 500TB
	)
	mock_disk_usage.return_value = large_usage

	result = drive_classifier.get_drive_usage("/dev/huge")

	assert result is not None
	assert result["total"] == 10**15
	assert result["used"] == 5 * 10**14
	assert result["free"] == 5 * 10**14


# --------------------------
# Tests - Get Drive Info
# --------------------------
@patch("psutil.disk_partitions")
def test_get_drive_info_multiple_partitions(
	mock_disk_partitions: Mock,
	mock_partitions_mixed: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test get_drive_info with multiple partitions.

	Verifies
	--------
	- All partitions are returned in the list
	- Each partition has all required keys
	- Values match the source partition data
	- Return type is correct

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_mixed : list[Mock]
		Mixed partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_mixed

	result = drive_classifier.get_drive_info()

	assert isinstance(result, list)
	assert len(result) == 4
	for drive_info in result:
		assert isinstance(drive_info, dict)
		assert set(drive_info.keys()) == {"device", "mountpoint", "fstype", "opts"}
		assert isinstance(drive_info["device"], str)
		assert isinstance(drive_info["mountpoint"], str)
		assert isinstance(drive_info["fstype"], str)
		assert isinstance(drive_info["opts"], str)


@patch("psutil.disk_partitions")
def test_get_drive_info_empty_partitions(
	mock_disk_partitions: Mock,
	mock_partitions_empty: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test get_drive_info with no partitions.

	Verifies
	--------
	- Empty partition list returns empty result
	- Return type is still a list
	- Method handles empty input gracefully

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_empty : list[Mock]
		Empty partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_empty

	result = drive_classifier.get_drive_info()

	assert isinstance(result, list)
	assert len(result) == 0


@patch("psutil.disk_partitions")
def test_get_drive_info_unicode_paths(
	mock_disk_partitions: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test get_drive_info with unicode characters in paths.

	Verifies
	--------
	- Unicode characters in paths are handled correctly
	- International characters are preserved
	- Special characters don't cause issues

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	unicode_partitions = [
		create_mock_partition("/dev/sda1", "/mnt/测试", "ext4", "rw,relatime"),
		create_mock_partition("/dev/sdb1", "/home/用户", "ntfs", "rw,uid=1000"),
	]
	mock_disk_partitions.return_value = unicode_partitions

	result = drive_classifier.get_drive_info()

	assert len(result) == 2
	assert result[0]["mountpoint"] == "/mnt/测试"
	assert result[1]["mountpoint"] == "/home/用户"


# --------------------------
# Tests - List All Drives
# --------------------------
@patch("psutil.disk_partitions")
def test_list_all_drives_multiple_drives(
	mock_disk_partitions: Mock,
	mock_partitions_mixed: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test list_all_drives with multiple drives.

	Verifies
	--------
	- All drive paths are returned
	- Order is preserved from psutil
	- Return type is list of strings
	- No classification is applied

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_mixed : list[Mock]
		Mixed partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_mixed

	result = drive_classifier.list_all_drives()

	assert isinstance(result, list)
	assert len(result) == 4
	assert all(isinstance(drive, str) for drive in result)
	expected_drives = ["/dev/sda1", "/dev/sdb1", "C:\\", "D:\\"]
	assert result == expected_drives


@patch("psutil.disk_partitions")
def test_list_all_drives_empty(
	mock_disk_partitions: Mock,
	mock_partitions_empty: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test list_all_drives with no drives.

	Verifies
	--------
	- Empty partition list returns empty drive list
	- Return type is still a list
	- Method handles empty input gracefully

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_empty : list[Mock]
		Empty partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_empty

	result = drive_classifier.list_all_drives()

	assert isinstance(result, list)
	assert len(result) == 0


@patch("psutil.disk_partitions")
def test_list_all_drives_single_drive(
	mock_disk_partitions: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test list_all_drives with single drive.

	Verifies
	--------
	- Single drive is returned in list
	- Return type and structure are correct
	- Edge case of single drive is handled

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	single_partition = [create_mock_partition("/dev/sda1", "/", "ext4", "rw")]
	mock_disk_partitions.return_value = single_partition

	result = drive_classifier.list_all_drives()

	assert isinstance(result, list)
	assert len(result) == 1
	assert result[0] == "/dev/sda1"


# --------------------------
# Tests - Filter Drives by FS
# --------------------------
@patch("psutil.disk_partitions")
def test_filter_drives_by_fs_existing_type(
	mock_disk_partitions: Mock,
	mock_partitions_mixed: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test filter_drives_by_fs with existing filesystem type.

	Verifies
	--------
	- Drives with matching filesystem are returned
	- Non-matching drives are excluded
	- Filtering works correctly with exact string match

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_mixed : list[Mock]
		Mixed partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_mixed

	result = drive_classifier.filter_drives_by_fs("ext4")

	assert isinstance(result, list)
	assert len(result) == 1
	assert "/dev/sda1" in result


@patch("psutil.disk_partitions")
def test_filter_drives_by_fs_nonexistent_type(
	mock_disk_partitions: Mock,
	mock_partitions_mixed: list[Mock],
	drive_classifier: DriveClassifier,
) -> None:
	"""Test filter_drives_by_fs with nonexistent filesystem type.

	Verifies
	--------
	- Nonexistent filesystem type returns empty list
	- No drives are returned for unmatched type
	- Method handles no matches gracefully

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	mock_partitions_mixed : list[Mock]
		Mixed partition fixtures
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	mock_disk_partitions.return_value = mock_partitions_mixed

	result = drive_classifier.filter_drives_by_fs("btrfs")

	assert isinstance(result, list)
	assert len(result) == 0


@patch("psutil.disk_partitions")
def test_filter_drives_by_fs_multiple_matches(
	mock_disk_partitions: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test filter_drives_by_fs with multiple matching drives.

	Verifies
	--------
	- All drives with matching filesystem are returned
	- Multiple matches are handled correctly
	- Order is preserved

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	ext4_partitions = [
		create_mock_partition("/dev/sda1", "/", "ext4", "rw"),
		create_mock_partition("/dev/sda2", "/home", "ext4", "rw"),
		create_mock_partition("/dev/sdb1", "/media", "vfat", "rw"),
	]
	mock_disk_partitions.return_value = ext4_partitions

	result = drive_classifier.filter_drives_by_fs("ext4")

	assert isinstance(result, list)
	assert len(result) == 2
	assert "/dev/sda1" in result
	assert "/dev/sda2" in result
	assert "/dev/sdb1" not in result


@patch("psutil.disk_partitions")
def test_filter_drives_by_fs_case_sensitive(
	mock_disk_partitions: Mock,
	drive_classifier: DriveClassifier,
) -> None:
	"""Test filter_drives_by_fs case sensitivity.

	Verifies
	--------
	- Filesystem filtering is case sensitive
	- Different case doesn't match
	- Exact string match is required

	Parameters
	----------
	mock_disk_partitions : Mock
		Mock for psutil.disk_partitions
	drive_classifier : DriveClassifier
		DriveClassifier instance from fixture

	Returns
	-------
	None
	"""
	ntfs_partitions = [
		create_mock_partition("C:\\", "C:\\", "NTFS", "rw"),
		create_mock_partition("D:\\", "D:\\", "ntfs", "rw"),
	]
	mock_disk_partitions.return_value = ntfs_partitions

	result_upper = drive_classifier.filter_drives_by_fs("NTFS")
	result_lower = drive_classifier.filter_drives_by_fs("ntfs")

	assert len(result_upper) == 1
	assert len(result_lower) == 1
	assert "C:\\" in result_upper
	assert len
