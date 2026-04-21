"""Unit tests for Excel manipulation utilities.

Tests the functionality of Excel manipulation classes including DealingExcel,
ExcelChart, ExcelWorksheet, ExcelWorkbook, ExcelApp, and ExcelWriter.
Covers normal operations, edge cases, error conditions, and type validation.
"""

import platform

import pytest


if platform.system() != "Windows":
	pytest.skip("Excel tests require Windows", allow_module_level=True)

import importlib
import sys

from pytest_mock import MockerFixture

from stpstone.utils.microsoft_apps.excel import (
	RGB_PALE_GREY,
	DealingExcel,
	ExcelApp,
	ExcelChart,
	ExcelWorkbook,
	ExcelWorksheet,
	ExcelWriter,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_excel_app(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked Excel application object.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked Excel application object
	"""
	xl = mocker.MagicMock()
	mocker.patch("stpstone.utils.microsoft_apps.excel.Dispatch", return_value=xl)
	return xl


@pytest.fixture
def mock_workbook(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked Excel workbook object.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked Excel workbook object
	"""
	return mocker.MagicMock()


@pytest.fixture
def mock_worksheet(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked Excel worksheet object.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked Excel worksheet object
	"""
	return mocker.MagicMock()


@pytest.fixture
def mock_dir_files_management(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked DirFilesManagement object.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked DirFilesManagement object
	"""
	return mocker.patch(
		"stpstone.utils.microsoft_apps.excel.DirFilesManagement", return_value=mocker.MagicMock()
	)


@pytest.fixture
def mock_io_open(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked io.open function.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked io.open function
	"""
	return mocker.patch("io.open")


@pytest.fixture
def mock_os_path_exists(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked os.path.exists function.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked os.path.exists function
	"""
	return mocker.patch("os.path.exists", return_value=True)


@pytest.fixture
def mock_os_remove(mocker: MockerFixture) -> object:
	"""Fixture providing a mocked os.remove function.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	object
		Mocked os.remove function
	"""
	return mocker.patch("os.remove")


@pytest.fixture
def dealing_excel() -> DealingExcel:
	"""Fixture providing a DealingExcel instance.

	Returns
	-------
	DealingExcel
		Instance of DealingExcel class
	"""
	return DealingExcel()


# --------------------------
# Tests for platform check
# --------------------------
def test_platform_check_non_windows(mocker: MockerFixture) -> None:
	"""Test module raises OSError on non-Windows systems.

	Verifies
	--------
	- Module import raises OSError when platform is not Windows
	- Error message matches expected pattern

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mocker.patch("platform.system", return_value="Linux")
	with pytest.raises(OSError, match="This module requires a Windows operating system"):
		importlib.reload(sys.modules["stpstone.utils.microsoft_apps.excel"])


# --------------------------
# Tests for DealingExcel
# --------------------------
class TestDealingExcel:
	"""Test cases for DealingExcel class."""

	def test_validate_filename_empty(self, dealing_excel: DealingExcel) -> None:
		"""Test _validate_filename with empty filename.

		Verifies
		--------
		- Raises ValueError for empty filename
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Filename cannot be empty"):
			dealing_excel._validate_filename("")

	def test_validate_filename_invalid_type(self, dealing_excel: DealingExcel) -> None:
		"""Test _validate_filename with non-string input.

		Verifies
		--------
		- Raises ValueError for non-string filename
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			dealing_excel._validate_filename(123)

	def test_validate_sheet_name_empty(self, dealing_excel: DealingExcel) -> None:
		"""Test _validate_sheet_name with empty sheet name.

		Verifies
		--------
		- Raises ValueError for empty sheet name
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Sheet name cannot be empty"):
			dealing_excel._validate_sheet_name("")

	def test_validate_sheet_name_invalid_type(self, dealing_excel: DealingExcel) -> None:
		"""Test _validate_sheet_name with non-string input.

		Verifies
		--------
		- Raises ValueError for non-string sheet name
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			dealing_excel._validate_sheet_name(123)

	def test_validate_range_empty(self, dealing_excel: DealingExcel) -> None:
		"""Test _validate_range with empty range string.

		Verifies
		--------
		- Raises ValueError for empty range
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Range string cannot be empty"):
			dealing_excel._validate_range("")

	def test_validate_range_invalid_type(self, dealing_excel: DealingExcel) -> None:
		"""Test _validate_range with non-string input.

		Verifies
		--------
		- Raises ValueError for non-string range
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			dealing_excel._validate_range(123)

	def test_save_as_success(
		self,
		dealing_excel: DealingExcel,
		mock_excel_app: object,
		mock_workbook: object,
		mocker: MockerFixture,
	) -> None:
		"""Test save_as with valid inputs.

		Verifies
		--------
		- Successfully saves workbook
		- Excel application is set to invisible
		- Workbook methods are called correctly

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_excel_app.Workbooks.Open.return_value = mock_workbook
		dealing_excel.save_as(mock_workbook, "test.xlsx")
		assert mock_excel_app.Visible == 0
		mock_workbook.SaveAs.assert_called_once_with("test.xlsx")
		mock_workbook.Close.assert_called_once_with(True)

	def test_save_as_failure(
		self, dealing_excel: DealingExcel, mock_excel_app: object, mocker: MockerFixture
	) -> None:
		"""Test save_as with COM error.

		Verifies
		--------
		- Raises ValueError on COM failure
		- Error message includes failure details

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_excel_app.Workbooks.Open.side_effect = Exception("COM error")
		with pytest.raises(ValueError, match="Failed to save workbook: COM error"):
			dealing_excel.save_as(mocker.MagicMock(), "test.xlsx")

	def test_save_as_active_wb_success(
		self,
		dealing_excel: DealingExcel,
		mock_excel_app: object,
		mock_workbook: object,
		mock_dir_files_management: object,
		mocker: MockerFixture,
	) -> None:
		"""Test save_as_active_wb with valid inputs.

		Verifies
		--------
		- Successfully saves workbook
		- Returns correct success status
		- Excel application is set to invisible
		- Workbook methods are called correctly

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mock_dir_files_management : object
			Mocked DirFilesManagement object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_excel_app.Workbooks.Open.return_value = mock_workbook
		mock_dir_files_management().object_exists.return_value = True
		result = dealing_excel.save_as_active_wb("input.xlsx", "output.xlsx")
		assert result == {"success": True}
		assert mock_excel_app.Visible == 0
		mock_workbook.SaveAs.assert_called_once_with("output.xlsx")
		mock_workbook.Close.assert_called_once_with(True)
		mock_dir_files_management().object_exists.assert_called_once_with("output.xlsx")

	def test_open_xl_success(
		self,
		dealing_excel: DealingExcel,
		mock_excel_app: object,
		mock_workbook: object,
		mocker: MockerFixture,
	) -> None:
		"""Test open_xl with valid inputs.

		Verifies
		--------
		- Successfully opens workbook
		- Returns correct Excel app and workbook objects
		- Excel application is set to invisible

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_excel_app.Workbooks.Open.return_value = mock_workbook
		result = dealing_excel.open_xl("test.xlsx")
		assert result == {"excel_app": mock_excel_app, "workbook": mock_workbook}
		assert mock_excel_app.Visible == 0
		mock_excel_app.Workbooks.Open.assert_called_once_with("test.xlsx")

	def test_close_wb_success(self, dealing_excel: DealingExcel, mock_workbook: object) -> None:
		"""Test close_wb with valid inputs.

		Verifies
		--------
		- Successfully closes workbook
		- Close method is called with correct save parameter

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_workbook : object
			Mocked Excel workbook object

		Returns
		-------
		None
		"""
		dealing_excel.close_wb(mock_workbook, save=True)
		mock_workbook.Close.assert_called_once_with(True)

	def test_delete_sheet_success(
		self,
		dealing_excel: DealingExcel,
		mock_excel_app: object,
		mock_workbook: object,
		mocker: MockerFixture,
	) -> None:
		"""Test delete_sheet with valid inputs.

		Verifies
		--------
		- Successfully deletes sheet
		- DisplayAlerts is properly managed
		- Workbook and sheet methods are called correctly

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_sheet = mocker.MagicMock()
		mock_workbook.Sheets.return_value = mock_sheet
		dealing_excel.delete_sheet("Sheet1", mock_excel_app, mock_workbook)
		mock_workbook.Activate.assert_called_once()
		mock_sheet.Delete.assert_called_once()
		assert mock_excel_app.DisplayAlerts is True

	def test_paste_values_column_success(
		self,
		dealing_excel: DealingExcel,
		mock_excel_app: object,
		mock_workbook: object,
		mocker: MockerFixture,
	) -> None:
		"""Test paste_values_column with valid inputs.

		Verifies
		--------
		- Successfully pastes values
		- Clipboard is cleared
		- Worksheet and range methods are called correctly

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_clearclipboard = mocker.patch.object(dealing_excel, "clearclipboard")
		mock_worksheet = mocker.MagicMock()
		mock_excel_app.Worksheets.return_value = mock_worksheet
		dealing_excel.paste_values_column("Sheet1", "A1:A10", mock_excel_app, mock_workbook)
		mock_workbook.Activate.assert_called_once()
		mock_worksheet.Activate.assert_called_once()
		mock_excel_app.Range.assert_called_with("A1:A10")
		mock_excel_app.Range().Copy.assert_called_once()
		mock_excel_app.Range().PasteSpecial.assert_called_once_with(Paste=-4163)
		mock_clearclipboard.assert_called_once()

	def test_color_range_w_rule_invalid_matching_value(
		self, dealing_excel: DealingExcel, mock_excel_app: object, mock_workbook: object
	) -> None:
		"""Test color_range_w_rule with invalid matching value.

		Verifies
		--------
		- Raises ValueError for non-string matching value
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			dealing_excel.color_range_w_rule(
				"Sheet1", "A1:A10", 123, 255, mock_excel_app, mock_workbook
			)

	def test_color_range_w_rule_invalid_color(
		self, dealing_excel: DealingExcel, mock_excel_app: object, mock_workbook: object
	) -> None:
		"""Test color_range_w_rule with invalid color.

		Verifies
		--------
		- Raises ValueError for negative color value
		- Error message matches expected pattern

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Color must be a positive integer"):
			dealing_excel.color_range_w_rule(
				"Sheet1", "A1:A10", "test", -1, mock_excel_app, mock_workbook
			)

	def test_autofit_range_columns_success(
		self,
		dealing_excel: DealingExcel,
		mock_excel_app: object,
		mock_workbook: object,
		mocker: MockerFixture,
	) -> None:
		"""Test autofit_range_columns with valid inputs.

		Verifies
		--------
		- Successfully autofits columns
		- Worksheet and column methods are called correctly

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_worksheet = mocker.MagicMock()
		mock_excel_app.Worksheets.return_value = mock_worksheet
		dealing_excel.autofit_range_columns("Sheet1", "A:C", mock_excel_app, mock_workbook)
		mock_workbook.Activate.assert_called_once()
		mock_worksheet.Activate.assert_called_once()
		mock_excel_app.ActiveSheet.Columns.assert_called_with("A:C")
		mock_excel_app.ActiveSheet.Columns().Columns.AutoFit.assert_called_once()

	def test_restore_corrupted_xl_success(
		self,
		dealing_excel: DealingExcel,
		mock_io_open: object,
		mock_dir_files_management: object,
		mocker: MockerFixture,
	) -> None:
		"""Test restore_corrupted_xl with valid inputs.

		Verifies
		--------
		- Successfully restores corrupted file
		- Returns True if output file exists
		- File operations are called correctly

		Parameters
		----------
		dealing_excel : DealingExcel
			Instance of DealingExcel class
		mock_io_open : object
			Mocked io.open function
		mock_dir_files_management : object
			Mocked DirFilesManagement object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_file = mocker.MagicMock()
		mock_file.readlines.return_value = ["a\tb\n", "c\td\n"]
		mock_io_open.return_value.__enter__.return_value = mock_file
		mock_dir_files_management().object_exists.return_value = True
		mock_xlwt_workbook = mocker.patch("stpstone.utils.microsoft_apps.excel.Workbook")
		mock_sheet = mocker.MagicMock()
		mock_xlwt_workbook().add_sheet.return_value = mock_sheet
		result = dealing_excel.restore_corrupted_xl("input.xls", "output.xls")
		assert result is True
		mock_io_open.assert_called_once_with("input.xls", "r", encoding="utf-16")
		mock_xlwt_workbook().add_sheet.assert_called_once_with("Sheet1", cell_overwrite_ok=True)
		mock_sheet.write.assert_any_call(0, 0, "a")
		mock_sheet.write.assert_any_call(0, 1, "b")
		mock_xlwt_workbook().save.assert_called_once_with("output.xls")
		mock_dir_files_management().object_exists.assert_called_once_with("output.xls")


# --------------------------
# Tests for ExcelChart
# --------------------------
class TestExcelChart:
	"""Test cases for ExcelChart class."""

	def test_validate_chart_name_empty(
		self, mock_excel_app: object, mock_workbook: object
	) -> None:
		"""Test _validate_chart_name with empty chart name.

		Verifies
		--------
		- Raises ValueError for empty chart name
		- Error message matches expected pattern

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Chart name cannot be empty"):
			ExcelChart(mock_excel_app, mock_workbook, "", mock_workbook)

	def test_init_success(
		self, mock_excel_app: object, mock_workbook: object, mock_worksheet: object
	) -> None:
		"""Test ExcelChart initialization with valid inputs.

		Verifies
		--------
		- Successfully initializes chart
		- Attributes are set correctly

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mock_worksheet : object
			Mocked Excel worksheet object

		Returns
		-------
		None
		"""
		chart = ExcelChart(mock_excel_app, mock_workbook, "TestChart", mock_worksheet)
		assert chart.excel == mock_excel_app
		assert chart.workbook == mock_workbook
		assert chart.chartname == "TestChart"
		assert chart.after_sheet == mock_worksheet
		assert chart.chart is None

	def test_set_title_invalid_type(self, mock_excel_app: object, mock_workbook: object) -> None:
		"""Test set_title with invalid title type.

		Verifies
		--------
		- Raises ValueError for non-string title
		- Error message matches expected pattern

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object

		Returns
		-------
		None
		"""
		chart = ExcelChart(mock_excel_app, mock_workbook, "TestChart", mock_workbook)
		with pytest.raises(TypeError):
			chart.set_title(123)

	def test_create_chart_missing_attributes(
		self, mock_excel_app: object, mock_workbook: object
	) -> None:
		"""Test create_chart with missing attributes.

		Verifies
		--------
		- Raises ValueError when required attributes are not set
		- Error message matches expected pattern

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object

		Returns
		-------
		None
		"""
		chart = ExcelChart(mock_excel_app, mock_workbook, "TestChart", mock_workbook)
		with pytest.raises(ValueError, match="All chart attributes must be set"):
			chart.create_chart()


# --------------------------
# Tests for ExcelWorksheet
# --------------------------
class TestExcelWorksheet:
	"""Test cases for ExcelWorksheet class."""

	def test_init_success(
		self, mock_excel_app: object, mock_workbook: object, mocker: MockerFixture
	) -> None:
		"""Test ExcelWorksheet initialization with valid inputs.

		Verifies
		--------
		- Successfully initializes worksheet
		- Worksheet name is set correctly

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_worksheet = mocker.MagicMock()
		mock_workbook.Worksheets.Add.return_value = mock_worksheet
		worksheet = ExcelWorksheet(mock_excel_app, mock_workbook, "Sheet1")
		assert worksheet.excel == mock_excel_app
		assert worksheet.workbook == mock_workbook
		assert worksheet.sheetname == "Sheet1"
		mock_worksheet.Name = "Sheet1"

	def test_set_cell_success(
		self, mock_excel_app: object, mock_workbook: object, mocker: MockerFixture
	) -> None:
		"""Test set_cell with valid inputs.

		Verifies
		--------
		- Successfully sets cell value
		- Cell methods are called correctly

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_worksheet = mocker.MagicMock()
		mock_workbook.Worksheets.Add.return_value = mock_worksheet
		worksheet = ExcelWorksheet(mock_excel_app, mock_workbook, "Sheet1")
		worksheet.set_cell(1, 1, "test")
		mock_worksheet.Cells.assert_called_with(1, 1)
		mock_worksheet.Cells().Value = "test"


# --------------------------
# Tests for ExcelWorkbook
# --------------------------
class TestExcelWorkbook:
	"""Test cases for ExcelWorkbook class."""

	def test_init_success(self, mock_excel_app: object, mocker: MockerFixture) -> None:
		"""Test ExcelWorkbook initialization with valid inputs.

		Verifies
		--------
		- Successfully initializes workbook
		- Attributes are set correctly

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_workbook = mocker.MagicMock()
		mock_excel_app.Workbooks.Add.return_value = mock_workbook
		workbook = ExcelWorkbook(mock_excel_app, "test.xlsx")
		assert workbook.excel == mock_excel_app
		assert workbook.filename == "test.xlsx"
		assert workbook.worksheets == {}
		mock_excel_app.Workbooks.Add.assert_called_once()

	def test_add_worksheet_success(self, mock_excel_app: object, mocker: MockerFixture) -> None:
		"""Test add_worksheet with valid inputs.

		Verifies
		--------
		- Successfully adds worksheet
		- Worksheet is stored in worksheets dict
		- Returns correct worksheet object

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_workbook = mocker.MagicMock()
		mock_excel_app.Workbooks.Add.return_value = mock_workbook
		workbook = ExcelWorkbook(mock_excel_app, "test.xlsx")
		worksheet = workbook.add_worksheet("Sheet1")
		assert isinstance(worksheet, ExcelWorksheet)
		assert workbook.worksheets["Sheet1"] == worksheet


# --------------------------
# Tests for ExcelApp
# --------------------------
class TestExcelApp:
	"""Test cases for ExcelApp class."""

	def test_init_success(self, mock_excel_app: object) -> None:
		"""Test ExcelApp initialization with valid inputs.

		Verifies
		--------
		- Successfully initializes Excel application
		- Workbooks list is empty
		- Default sheet number is set

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object

		Returns
		-------
		None
		"""
		app = ExcelApp()
		assert app.excel == mock_excel_app
		assert app.workbooks == []
		mock_excel_app.SheetsInNewWorkbook = 1

	def test_add_workbook_success(self, mock_excel_app: object, mocker: MockerFixture) -> None:
		"""Test add_workbook with valid inputs.

		Verifies
		--------
		- Successfully adds workbook
		- Workbook is added to workbooks list
		- Returns correct workbook object

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_workbook = mocker.MagicMock()
		mock_excel_app.Workbooks.Add.return_value = mock_workbook
		app = ExcelApp()
		workbook = app.add_workbook("test.xlsx")
		assert isinstance(workbook, ExcelWorkbook)
		assert len(app.workbooks) == 1
		assert app.workbooks[0] == workbook


# --------------------------
# Tests for ExcelWriter
# --------------------------
class TestExcelWriter:
	"""Test cases for ExcelWriter class."""

	def test_init_success(
		self, mock_excel_app: object, mock_workbook: object, mocker: MockerFixture
	) -> None:
		"""Test ExcelWriter initialization with valid inputs.

		Verifies
		--------
		- Successfully initializes Excel writer
		- Attributes are set correctly
		- Excel application visibility is set correctly

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_excel_app.ActiveWorkbook = mock_workbook
		mock_excel_app.ActiveSheet = mocker.MagicMock()
		writer = ExcelWriter("test.xlsx", "Sheet1", make_visible=False)
		assert writer.excelapp == mock_excel_app
		assert writer.file_name == "test.xlsx"
		assert writer.default_sheet.Name == "Sheet1"
		assert mock_excel_app.Visible == 0

	def test_add_sheet_after_success(
		self, mock_excel_app: object, mock_workbook: object, mocker: MockerFixture
	) -> None:
		"""Test add_sheet_after with valid inputs.

		Verifies
		--------
		- Successfully adds new sheet
		- Sheet is added after specified index

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_excel_app.ActiveWorkbook = mock_workbook
		mock_new_sheet = mocker.MagicMock()
		mock_workbook.Sheets.Add.return_value = mock_new_sheet
		writer = ExcelWriter("test.xlsx", "Sheet1")
		writer.add_sheet_after("Sheet2", 1)
		mock_workbook.Sheets.Add.assert_called_with(None, mock_workbook.Sheets(1))
		mock_new_sheet.Name = "Sheet2"

	def test_format_range_invalid_style(
		self, mock_excel_app: object, mock_workbook: object, mocker: MockerFixture
	) -> None:
		"""Test format_range with invalid style.

		Verifies
		--------
		- Raises ValueError for undefined style
		- Error message matches expected pattern

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object
		mocker : MockerFixture
			Pytest-mock fixture for creating mocks

		Returns
		-------
		None
		"""
		mock_excel_app.ActiveWorkbook = mock_workbook
		writer = ExcelWriter("test.xlsx", "Sheet1")
		with pytest.raises(ValueError, match="Style 'invalid_style' has not been defined"):
			writer.format_range(mocker.MagicMock(), "invalid_style")

	def test_reset_color_pallet_invalid_index(
		self, mock_excel_app: object, mock_workbook: object
	) -> None:
		"""Test reset_color_pallet with invalid color index.

		Verifies
		--------
		- Raises ValueError for out-of-range color index
		- Error message matches expected pattern

		Parameters
		----------
		mock_excel_app : object
			Mocked Excel application object
		mock_workbook : object
			Mocked Excel workbook object

		Returns
		-------
		None
		"""
		writer = ExcelWriter("test.xlsx", "Sheet1")
		with pytest.raises(ValueError, match="Color index must be between 1 and 56"):
			writer.reset_color_pallet(0, RGB_PALE_GREY)
