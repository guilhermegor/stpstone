"""Excel manipulation utilities using win32com and xlwt.

This module provides classes for creating, formatting, and manipulating Excel spreadsheets
using win32com for COM automation and xlwt for writing Excel files. It includes functionality
for handling workbooks, worksheets, charts, and cell formatting with robust error handling.
"""

import platform


if platform.system() != "Windows":
    raise OSError("This module requires a Windows operating system to function properly.")

from ctypes import windll
import os
from typing import Literal, Optional, TypedDict, TypeVar, Union

from win32com.client import Dispatch, constants
from xlwt import Workbook

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.folders import DirFilesManagement


STYLE_HEADING1: Literal["style_heading1"] = "style_heading1"
STYLE_HEADING2: Literal["style_heading2"] = "style_heading2"
STYLE_BORDER_BOTTOM: Literal["style_border_bottom"] = "style_border_bottom"
STYLE_GREY_CELL: Literal["style_grey_cell"] = "style_grey_cell"
STYLE_PALE_YELLOW_CELL: Literal["style_pale_yellow_cell"] = "style_pale_yellow_cell"
STYLE_ITALICS: Literal["style_italics"] = "style_italics"

XL_CONST_EDGE_LEFT: int = 7
XL_CONST_EDGE_BOTTOM: int = 9
XL_CONST_CONTINUOUS: int = 1
XL_CONST_AUTOMATIC: int = -4105
XL_CONST_THIN: int = 2
XL_CONST_GRAY16: int = 17
XL_CONST_SOLID: int = 1
RGB_PALE_GREY: int = 15132390
RGB_PALE_YELLOW: int = 13565951


TypeStyleXLObject = TypeVar("TypeVarStyle", 
                      Literal["style_heading1", "style_heading2", "style_border_bottom", 
                              "style_grey_cell", "style_pale_yellow_cell", "style_italics"])

class SaveAsReturn(TypedDict):
    """Return type for save_as_active_wb method.

    Returns
    -------
    bool
        True if file exists after saving, False otherwise
    """

    success: bool


class OpenXlReturn(TypedDict):
    """Return type for open_xl method.

    Returns
    -------
    tuple
        Excel application object and opened workbook
    """

    excel_app: object
    workbook: object


class DealingExcel(metaclass=TypeChecker):
    """Class for handling Excel workbook operations and formatting."""

    def _validate_filename(self, filename: str) -> None:
        """Validate filename for Excel operations.

        Parameters
        ----------
        filename : str
            File path to validate

        Raises
        ------
        ValueError
            If filename is empty or not a string
        """
        if not filename:
            raise ValueError("Filename cannot be empty")
        if not isinstance(filename, str):
            raise ValueError("Filename must be a string")

    def _validate_sheet_name(self, sheet_name: str) -> None:
        """Validate sheet name for Excel operations.

        Parameters
        ----------
        sheet_name : str
            Sheet name to validate

        Raises
        ------
        ValueError
            If sheet name is empty or not a string
        """
        if not sheet_name:
            raise ValueError("Sheet name cannot be empty")
        if not isinstance(sheet_name, str):
            raise ValueError("Sheet name must be a string")

    def _validate_range(self, str_range: str) -> None:
        """Validate Excel range string.

        Parameters
        ----------
        str_range : str
            Excel range string to validate

        Raises
        ------
        ValueError
            If range is empty or not a string
        """
        if not str_range:
            raise ValueError("Range string cannot be empty")
        if not isinstance(str_range, str):
            raise ValueError("Range string must be a string")

    def save_as(self, active_wb: object, filename: str) -> None:
        """Save workbook with specified filename.

        Parameters
        ----------
        active_wb : object
            Active Excel workbook object
        filename : str
            Full path to save the Excel file

        Raises
        ------
        ValueError
            If filename is invalid or save operation fails
        """
        self._validate_filename(filename)
        try:
            xlapp = Dispatch("Excel.Application")
            xlapp.Visible = 0
            wb = xlapp.Workbooks.Open(active_wb)
            wb.SaveAs(filename)
            wb.Close(True)
        except Exception as err:
            raise ValueError(f"Failed to save workbook: {str(err)}") from err

    def save_as_active_wb(self, filename: str, filename_as: str) -> SaveAsReturn:
        """Save workbook with a new filename and verify existence.

        Parameters
        ----------
        filename : str
            Original workbook file path
        filename_as : str
            New file path to save as

        Returns
        -------
        SaveAsReturn
            Dictionary with success status

        Raises
        ------
        ValueError
            If filenames are invalid or save operation fails
        """
        self._validate_filename(filename)
        self._validate_filename(filename_as)
        try:
            xlapp = Dispatch("Excel.Application")
            xlapp.Visible = 0
            wb = xlapp.Workbooks.Open(filename)
            wb.SaveAs(filename_as)
            wb.Close(True)
            return {"success": DirFilesManagement().object_exists(filename_as)}
        except Exception as err:
            raise ValueError(f"Failed to save workbook: {str(err)}") from err

    def open_xl(self, filename: str) -> OpenXlReturn:
        """Open an Excel workbook.

        Parameters
        ----------
        filename : str
            Full path to the Excel file

        Returns
        -------
        OpenXlReturn
            Dictionary containing Excel application and workbook objects

        Raises
        ------
        ValueError
            If filename is invalid or open operation fails
        """
        self._validate_filename(filename)
        try:
            xlapp = Dispatch("Excel.Application")
            xlapp.Visible = 0
            wb = xlapp.Workbooks.Open(filename)
            return {"excel_app": xlapp, "workbook": wb}
        except Exception as err:
            raise ValueError(f"Failed to open workbook: {str(err)}") from err

    def close_wb(self, active_wb: object, save: bool = True) -> None:
        """Close an active workbook.

        Parameters
        ----------
        active_wb : object
            Active Excel workbook object
        save : bool
            Whether to save before closing (default: True)

        Raises
        ------
        ValueError
            If close operation fails
        """
        try:
            active_wb.Close(save)
        except Exception as err:
            raise ValueError(f"Failed to close workbook: {str(err)}") from err

    def delete_sheet(self, sheet_name: str, excel_app: object, active_wb: object) -> None:
        """Delete a sheet from the workbook.

        Parameters
        ----------
        sheet_name : str
            Name of the sheet to delete
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If sheet name is invalid or delete operation fails
        """
        self._validate_sheet_name(sheet_name)
        try:
            active_wb.Activate()
            excel_app.DisplayAlerts = False
            active_wb.Sheets(sheet_name).Delete()
            excel_app.DisplayAlerts = True
        except Exception as err:
            raise ValueError(f"Failed to delete sheet: {str(err)}") from err

    def paste_values_column(
        self, 
        sheet_name: str, 
        str_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Paste values into a specified column.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range for pasting values
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or paste operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.Range(str_range).Copy()
            excel_app.Range(str_range).PasteSpecial(Paste=-4163)
            self.clearclipboard()
        except Exception as err:
            raise ValueError(f"Failed to paste values: {str(err)}") from err

    def color_range_w_rule(
        self, 
        sheet_name: str, 
        str_range: str, 
        matching_value: str, 
        color: int, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Color rows based on matching value in range.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range to check
        matching_value : str
            Value to match for coloring
        color : int
            RGB color value
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or coloring operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        if not isinstance(matching_value, str):
            raise ValueError("Matching value must be a string")
        if not isinstance(color, int) or color < 0:
            raise ValueError("Color must be a positive integer")
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            for cel in excel_app.ActiveSheet.Range(str_range).SpecialCells(2):
                if str(cel) == matching_value:
                    cel.EntireRow.Interior.Pattern = 1
                    cel.EntireRow.Interior.PatternColorIndex = -4105
                    cel.EntireRow.Interior.Color = color
                    cel.EntireRow.Interior.TintAndShade = 0
                    cel.EntireRow.Interior.PatternTintAndShade = 0
        except Exception as err:
            raise ValueError(f"Failed to color range: {str(err)}") from err

    def autofit_range_columns(
        self, 
        sheet_name: str, 
        str_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Autofit columns in specified range.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range for autofitting
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or autofit operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.ActiveSheet.Columns(str_range).Columns.AutoFit()
        except Exception as err:
            raise ValueError(f"Failed to autofit columns: {str(err)}") from err

    def delete_entire_column(
        self, 
        sheet_name: str, 
        str_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Delete specified column from worksheet.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range of column to delete
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or delete operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.ActiveSheet.Range(str_range).Columns.Delete()
        except Exception as err:
            raise ValueError(f"Failed to delete column: {str(err)}") from err

    def delete_cells_w_specific_data(
        self, 
        sheet_name: str, 
        str_range: str, 
        value_to_delete: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Delete rows containing specific value in range.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range to check
        value_to_delete : str
            Value to match for deletion
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or delete operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        if not isinstance(value_to_delete, str):
            raise ValueError("Value to delete must be a string")
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            for cel in excel_app.ActiveSheet.Range(str_range).SpecialCells(2):
                if str(cel) == value_to_delete:
                    excel_app.ActiveSheet.Cells(cel.row, cel.column + 1).value = "DELETAR"
            excel_app.Worksheets(sheet_name)\
                .Cells\
                .Find("DELETAR")\
                .EntireColumn\
                .SpecialCells(2)\
                .EntireRow.Delete()
        except Exception as err:
            raise ValueError(f"Failed to delete cells: {str(err)}") from err

    def replacing_all_matching_str_column(
        self, 
        sheet_name: str, 
        str_range: str, 
        value_to_replace: str, 
        replace_value: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Replace matching values in a column.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range to check
        value_to_replace : str
            Value to replace
        replace_value : str
            Replacement value
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or replace operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        if not isinstance(value_to_replace, str):
            raise ValueError("Value to replace must be a string")
        if not isinstance(replace_value, str):
            raise ValueError("Replacement value must be a string")
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            for cel in excel_app.ActiveSheet.Range(str_range).SpecialCells(2):
                if str(cel) == value_to_replace:
                    cel.value = replace_value
        except Exception as err:
            raise ValueError(f"Failed to replace values: {str(err)}") from err

    def create_new_column(
        self, 
        sheet_name: str, 
        str_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Create a new column before specified range.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range before which to insert column
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or insert operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.ActiveSheet.Columns(str_range).Insert()
        except Exception as err:
            raise ValueError(f"Failed to create new column: {str(err)}") from err

    def text_to_columns(
        self, 
        sheet_name: str, 
        str_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Convert text to columns in specified range.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range for text-to-columns operation
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.ActiveSheet.Range(str_range).TextToColumns()
        except Exception as err:
            raise ValueError(f"Failed to convert text to columns: {str(err)}") from err

    def conf_title(
        self, 
        sheet_name: str, 
        str_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Configure title formatting for specified range.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range for title formatting
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or formatting operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            range_obj = excel_app.Range(str_range)
            range_obj.Borders(-4131).LineStyle = -4142
            range_obj.Borders(-4152).LineStyle = -4142
            range_obj.Borders(-4160).LineStyle = -4142
            range_obj.Borders(-4107).LineStyle = -4142
            range_obj.Borders(5).LineStyle = -4142
            range_obj.Borders(6).LineStyle = -4142
            range_obj.Font.Bold = True
        except Exception as err:
            raise ValueError(f"Failed to configure title: {str(err)}") from err

    def copy_column_paste_in_other(
        self, 
        sheet_name: str, 
        source_range: str, 
        dest_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Copy column and paste to another location.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        source_range : str
            Source Excel range
        dest_range : str
            Destination Excel range
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or copy-paste operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(source_range)
        self._validate_range(dest_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.Range(source_range).Copy()
            excel_app.Range(dest_range).PasteSpecial(-4104)
            self.clearclipboard()
        except Exception as err:
            raise ValueError(f"Failed to copy and paste column: {str(err)}") from err

    def move_column(
        self, 
        sheet_name: str, 
        source_range: str, 
        dest_range: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Move column to another location.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        source_range : str
            Source Excel range
        dest_range : str
            Destination Excel range
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or move operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(source_range)
        self._validate_range(dest_range)
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.Columns(source_range).Cut()
            excel_app.Columns(dest_range).Insert()
            self.clearclipboard()
        except Exception as err:
            raise ValueError(f"Failed to move column: {str(err)}") from err

    def attr_value_cel(
        self, 
        sheet_name: str, 
        str_range: str, 
        value: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Set value for a specific cell.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range for cell
        value : str
            Value to set
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or set operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        if not isinstance(value, str):
            raise ValueError("Cell value must be a string")
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.Range(str_range).Value = value
        except Exception as err:
            raise ValueError(f"Failed to set cell value: {str(err)}") from err

    def number_format(
        self, 
        sheet_name: str, 
        str_range: str, 
        format_str: str, 
        excel_app: object, 
        active_wb: object
    ) -> None:
        """Set number format for specified range.

        Parameters
        ----------
        sheet_name : str
            Name of the target sheet
        str_range : str
            Excel range for formatting
        format_str : str
            Number format string
        excel_app : object
            Excel application object
        active_wb : object
            Active workbook object

        Raises
        ------
        ValueError
            If inputs are invalid or format operation fails
        """
        self._validate_sheet_name(sheet_name)
        self._validate_range(str_range)
        if not isinstance(format_str, str):
            raise ValueError("Format string must be a string")
        try:
            active_wb.Activate()
            excel_app.Worksheets(sheet_name).Activate()
            excel_app.Range(str_range).NumberFormat = format_str
        except Exception as err:
            raise ValueError(f"Failed to set number format: {str(err)}") from err

    def clearclipboard(self) -> None:
        """Clear the system clipboard.

        Raises
        ------
        ValueError
            If clipboard clearing fails
        """
        try:
            if windll.user32.OpenClipboard(None):
                windll.user32.EmptyClipboard()
                windll.user32.CloseClipboard()
        except Exception as err:
            raise ValueError(f"Failed to clear clipboard: {str(err)}") from err

    def restore_corrupted_xl(
        self, 
        filename_orig: str, 
        filename_output: str
    ) -> bool:
        """Restore corrupted Excel file by reading and rewriting as new file.

        Parameters
        ----------
        filename_orig : str
            Original corrupted file path
        filename_output : str
            Output file path

        Returns
        -------
        bool
            True if output file exists, False otherwise

        Raises
        ------
        ValueError
            If filenames are invalid or restore operation fails
        """
        self._validate_filename(filename_orig)
        self._validate_filename(filename_output)
        try:
            with open(filename_orig, encoding="utf-16") as f:
                data = f.readlines()
            xldoc = Workbook()
            sheet = xldoc.add_sheet("Sheet1", cell_overwrite_ok=True)
            for i, row in enumerate(data):
                for j, val in enumerate(row.replace("\n", "").split("\t")):
                    sheet.write(i, j, val)
            xldoc.save(filename_output)
            return DirFilesManagement().object_exists(filename_output)
        except Exception as err:
            raise ValueError(f"Failed to restore Excel file: {str(err)}") from err


class ExcelChart(metaclass=TypeChecker):
    """Class for creating and configuring Excel charts."""

    def _validate_chart_name(self, chart_name: str) -> None:
        """Validate chart name.

        Parameters
        ----------
        chart_name : str
            Chart name to validate

        Raises
        ------
        ValueError
            If chart name is empty or not a string
        """
        if not chart_name:
            raise ValueError("Chart name cannot be empty")
        if not isinstance(chart_name, str):
            raise ValueError("Chart name must be a string")

    def _validate_chart_type(self, chart_type: int) -> None:
        """Validate chart type.

        Parameters
        ----------
        chart_type : int
            Chart type constant

        Raises
        ------
        ValueError
            If chart type is not a positive integer
        """
        if not isinstance(chart_type, int) or chart_type <= 0:
            raise ValueError("Chart type must be a positive integer")

    def __init__(
        self, 
        excel: object, 
        workbook: object, 
        chartname: str, 
        after_sheet: object
    ) -> None:
        """Initialize Excel chart object.

        Parameters
        ----------
        excel : object
            Excel application object
        workbook : object
            Workbook object
        chartname : str
            Name of the chart
        after_sheet : object
            Sheet after which to add chart
        """
        self._validate_chart_name(chartname)
        self.excel = excel
        self.workbook = workbook
        self.chartname = chartname
        self.after_sheet = after_sheet
        self.chart: Optional[object] = None
        self.chartTitle: Optional[str] = None
        self.chartType: Optional[int] = None
        self.chartSource: Optional[object] = None
        self.plotBy: Optional[int] = None
        self.numCategoryLabels: Optional[int] = None
        self.numSeriesLabels: Optional[int] = None
        self.categoryTitle: Optional[str] = None
        self.valueTitle: Optional[str] = None

    def set_title(self, chart_title: str) -> None:
        """Set chart title.

        Parameters
        ----------
        chart_title : str
            Title for the chart

        Raises
        ------
        ValueError
            If chart title is invalid
        """
        if not isinstance(chart_title, str):
            raise ValueError("Chart title must be a string")
        self.chartTitle = chart_title

    def set_type(self, chart_type: int) -> None:
        """Set chart type.

        Parameters
        ----------
        chart_type : int
            Excel chart type constant
        """
        self._validate_chart_type(chart_type)
        self.chartType = chart_type

    def set_source(self, chart_source: object) -> None:
        """Set chart data source.

        Parameters
        ----------
        chart_source : object
            Excel range for chart data

        Raises
        ------
        ValueError
            If chart source is invalid
        """
        if chart_source is None:
            raise ValueError("Chart source cannot be None")
        self.chartSource = chart_source

    def set_plot_by(self, plot_by: int) -> None:
        """Set plot by option.

        Parameters
        ----------
        plot_by : int
            Plot by constant (rows or columns)

        Raises
        ------
        ValueError
            If plot by is invalid
        """
        if not isinstance(plot_by, int):
            raise ValueError("Plot by must be an integer")
        self.plotBy = plot_by

    def set_category_labels(self, num_category_labels: int) -> None:
        """Set number of category labels.

        Parameters
        ----------
        num_category_labels : int
            Number of category labels

        Raises
        ------
        ValueError
            If number of category labels is invalid
        """
        if not isinstance(num_category_labels, int) or num_category_labels < 0:
            raise ValueError("Number of category labels must be a non-negative integer")
        self.numCategoryLabels = num_category_labels

    def set_series_labels(self, num_series_labels: int) -> None:
        """Set number of series labels.

        Parameters
        ----------
        num_series_labels : int
            Number of series labels

        Raises
        ------
        ValueError
            If number of series labels is invalid
        """
        if not isinstance(num_series_labels, int) or num_series_labels < 0:
            raise ValueError("Number of series labels must be a non-negative integer")
        self.numSeriesLabels = num_series_labels

    def set_category_title(self, category_title: str) -> None:
        """Set category axis title.

        Parameters
        ----------
        category_title : str
            Title for category axis

        Raises
        ------
        ValueError
            If category title is invalid
        """
        if not isinstance(category_title, str):
            raise ValueError("Category title must be a string")
        self.categoryTitle = category_title

    def set_value_title(self, value_title: str) -> None:
        """Set value axis title.

        Parameters
        ----------
        value_title : str
            Title for value axis

        Raises
        ------
        ValueError
            If value title is invalid
        """
        if not isinstance(value_title, str):
            raise ValueError("Value title must be a string")
        self.valueTitle = value_title

    def create_chart(self) -> None:
        """Create and configure Excel chart.

        Raises
        ------
        ValueError
            If chart creation fails or required attributes are not set
        """
        if any(attr is None for attr in [self.chartTitle, self.chartType, self.chartSource, 
                                        self.plotBy, self.categoryTitle, self.valueTitle]):
            raise ValueError("All chart attributes must be set before creation")
        try:
            self.chart = self.workbook.Charts.Add(After=self.after_sheet)
            self.chart.ChartWizard(
                Gallery=constants.xlColumn,
                CategoryLabels=self.numCategoryLabels,
                SeriesLabels=self.numSeriesLabels,
                CategoryTitle=self.categoryTitle,
                ValueTitle=self.valueTitle,
                PlotBy=self.plotBy,
                Title=self.chartTitle
            )
            self.chart.SetSourceData(self.chartSource, self.plotBy)
            self.chart.HasAxis = (constants.xlCategory, constants.xlPrimary)
            self.chart.Axes(constants.xlCategory).HasTitle = 1
            self.chart.Axes(constants.xlCategory).AxisTitle.Text = self.categoryTitle
            self.chart.Axes(constants.xlValue).HasTitle = 1
            self.chart.Axes(constants.xlValue).AxisTitle.Text = self.valueTitle
            self.chart.Axes(constants.xlValue).AxisTitle.Orientation = constants.xlUpward
            self.chart.PlotBy = self.plotBy
            self.chart.Name = self.chartname
            self.chart.HasTitle = 1
            self.chart.ChartTitle.Text = self.chartTitle
            self.chart.HasDataTable = 0
            self.chart.ChartType = self.chartType
        except Exception as err:
            raise ValueError(f"Failed to create chart: {str(err)}") from err

    def set_legend_position(self, legend_position: int) -> None:
        """Set chart legend position.

        Parameters
        ----------
        legend_position : int
            Legend position constant

        Raises
        ------
        ValueError
            If legend position is invalid or chart not created
        """
        if self.chart is None:
            raise ValueError("Chart must be created before setting legend position")
        if not isinstance(legend_position, int):
            raise ValueError("Legend position must be an integer")
        try:
            self.chart.Legend.Position = legend_position
        except Exception as err:
            raise ValueError(f"Failed to set legend position: {str(err)}") from err

    def plot_by_columns(self) -> None:
        """Set chart to plot by columns.

        Raises
        ------
        ValueError
            If chart not created or plot operation fails
        """
        if self.chart is None:
            raise ValueError("Chart must be created before setting plot by")
        try:
            self.chart.PlotBy = constants.xlColumns
        except Exception as err:
            raise ValueError(f"Failed to set plot by columns: {str(err)}") from err

    def plot_by_rows(self) -> None:
        """Set chart to plot by rows.

        Raises
        ------
        ValueError
            If chart not created or plot operation fails
        """
        if self.chart is None:
            raise ValueError("Chart must be created before setting plot by")
        try:
            self.chart.PlotBy = constants.xlRows
        except Exception as err:
            raise ValueError(f"Failed to set plot by rows: {str(err)}") from err

    def set_category_axis_range(
        self, 
        min_value: float, 
        max_value: float
    ) -> None:
        """Set category axis range.

        Parameters
        ----------
        min_value : float
            Minimum value for category axis
        max_value : float
            Maximum value for category axis

        Raises
        ------
        ValueError
            If inputs are invalid or range setting fails
        """
        if self.chart is None:
            raise ValueError("Chart must be created before setting axis range")
        if not isinstance(min_value, (int, float)) or not isinstance(max_value, (int, float)):
            raise ValueError("Axis range values must be numeric")
        if min_value >= max_value:
            raise ValueError("Minimum value must be less than maximum value")
        try:
            self.chart.Axes(constants.xlCategory).MinimumScale = min_value
            self.chart.Axes(constants.xlCategory).MaximumScale = max_value
        except Exception as err:
            raise ValueError(f"Failed to set category axis range: {str(err)}") from err

    def set_value_axis_range(self, min_value: float, max_value: float) -> None:
        """Set value axis range.

        Parameters
        ----------
        min_value : float
            Minimum value for value axis
        max_value : float
            Maximum value for value axis

        Raises
        ------
        ValueError
            If inputs are invalid or range setting fails
        """
        if self.chart is None:
            raise ValueError("Chart must be created before setting axis range")
        if not isinstance(min_value, (int, float)) or not isinstance(max_value, (int, float)):
            raise ValueError("Axis range values must be numeric")
        if min_value >= max_value:
            raise ValueError("Minimum value must be less than maximum value")
        try:
            self.chart.Axes(constants.xlValue).MinimumScale = min_value
            self.chart.Axes(constants.xlValue).MaximumScale = max_value
        except Exception as err:
            raise ValueError(f"Failed to set value axis range: {str(err)}") from err

    def apply_data_labels(self, data_label_type: int) -> None:
        """Apply data labels to chart.

        Parameters
        ----------
        data_label_type : int
            Data label type constant

        Raises
        ------
        ValueError
            If chart not created or label application fails
        """
        if self.chart is None:
            raise ValueError("Chart must be created before applying data labels")
        if not isinstance(data_label_type, int):
            raise ValueError("Data label type must be an integer")
        try:
            self.chart.ApplyDataLabels(data_label_type)
        except Exception as err:
            raise ValueError(f"Failed to apply data labels: {str(err)}") from err

    def set_border_line_style(self, line_style: int) -> None:
        """Set border line style for chart plot area.

        Parameters
        ----------
        line_style : int
            Line style constant

        Raises
        ------
        ValueError
            If chart not created or style setting fails
        """
        if self.chart is None:
            raise ValueError("Chart must be created before setting border style")
        if not isinstance(line_style, int):
            raise ValueError("Line style must be an integer")
        try:
            self.chart.PlotArea.Border.LineStyle = line_style
        except Exception as err:
            raise ValueError(f"Failed to set border line style: {str(err)}") from err

    def set_interior_style(self, interior_style: int) -> None:
        """Set interior style for chart plot area.

        Parameters
        ----------
        interior_style : int
            Interior style constant

        Raises
        ------
        ValueError
            If chart not created or style setting fails
        """
        if self.chart is None:
            raise ValueError("Chart must be created before setting interior style")
        if not isinstance(interior_style, int):
            raise ValueError("Interior style must be an integer")
        try:
            self.chart.PlotArea.Interior.Pattern = interior_style
        except Exception as err:
            raise ValueError(f"Failed to set interior style: {str(err)}") from err


class ExcelWorksheet(metaclass=TypeChecker):
    """Class for managing Excel worksheets."""

    def _validate_sheet_name(self, sheet_name: str) -> None:
        """Validate worksheet name.

        Parameters
        ----------
        sheet_name : str
            Worksheet name to validate

        Raises
        ------
        ValueError
            If sheet name is empty or not a string
        """
        if not sheet_name:
            raise ValueError("Sheet name cannot be empty")
        if not isinstance(sheet_name, str):
            raise ValueError("Sheet name must be a string")

    def _validate_cell_position(self, row: int, col: int) -> None:
        """Validate cell row and column positions.

        Parameters
        ----------
        row : int
            Row number
        col : int
            Column number

        Raises
        ------
        ValueError
            If row or column is invalid
        """
        if not isinstance(row, int) or row < 1:
            raise ValueError("Row must be a positive integer")
        if not isinstance(col, int) or col < 1:
            raise ValueError("Column must be a positive integer")

    def __init__(self, excel: object, workbook: object, sheetname: str) -> None:
        """Initialize Excel worksheet.

        Parameters
        ----------
        excel : object
            Excel application object
        workbook : object
            Workbook object
        sheetname : str
            Name of the worksheet

        Raises
        ------
        ValueError
            If inputs are invalid or worksheet creation fails
        """
        self._validate_sheet_name(sheetname)
        self.excel = excel
        self.workbook = workbook
        self.sheetname = sheetname
        try:
            self.worksheet = self.workbook.Worksheets.Add()
            self.worksheet.Name = sheetname
        except Exception as err:
            raise ValueError(f"Failed to create worksheet: {str(err)}") from err

    def activate(self) -> None:
        """Activate the worksheet.

        Raises
        ------
        ValueError
            If activation fails
        """
        try:
            self.worksheet.Activate()
        except Exception as err:
            raise ValueError(f"Failed to activate worksheet: {str(err)}") from err

    def set_cell(self, row: int, col: int, value: str) -> None:
        """Set value for a specific cell.

        Parameters
        ----------
        row : int
            Row number
        col : int
            Column number
        value : str
            Value to set

        Raises
        ------
        ValueError
            If inputs are invalid or set operation fails
        """
        self._validate_cell_position(row, col)
        if not isinstance(value, str):
            raise ValueError("Cell value must be a string")
        try:
            self.worksheet.Cells(row, col).Value = value
        except Exception as err:
            raise ValueError(f"Failed to set cell value: {str(err)}") from err

    def get_cell(self, row: int, col: int) -> str:
        """Get value from a specific cell.

        Parameters
        ----------
        row : int
            Row number
        col : int
            Column number

        Returns
        -------
        str
            Cell value

        Raises
        ------
        ValueError
            If inputs are invalid or get operation fails
        """
        self._validate_cell_position(row, col)
        try:
            return self.worksheet.Cells(row, col).Value
        except Exception as err:
            raise ValueError(f"Failed to get cell value: {str(err)}") from err

    def set_font(
        self, 
        row: int, 
        col: int, 
        font: str, 
        size: float
    ) -> None:
        """Set font properties for a specific cell.

        Parameters
        ----------
        row : int
            Row number
        col : int
            Column number
        font : str
            Font name
        size : float
            Font size

        Raises
        ------
        ValueError
            If inputs are invalid or font setting fails
        """
        self._validate_cell_position(row, col)
        if not isinstance(font, str):
            raise ValueError("Font name must be a string")
        if not isinstance(size, (int, float)) or size <= 0:
            raise ValueError("Font size must be a positive number")
        try:
            self.worksheet.Cells(row, col).Font.Name = font
            self.worksheet.Cells(row, col).Font.Size = size
        except Exception as err:
            raise ValueError(f"Failed to set font: {str(err)}") from err

    def get_font(
        self, 
        row: int, 
        col: int
    ) -> tuple[str, float]:
        """Get font properties for a specific cell.

        Parameters
        ----------
        row : int
            Row number
        col : int
            Column number

        Returns
        -------
        tuple[str, float]
            Font name and size

        Raises
        ------
        ValueError
            If inputs are invalid or font retrieval fails
        """
        self._validate_cell_position(row, col)
        try:
            font = self.worksheet.Cells(row, col).Font.Name
            size = self.worksheet.Cells(row, col).Font.Size
            return (font, size)
        except Exception as err:
            raise ValueError(f"Failed to get font: {str(err)}") from err


class ExcelWorkbook(metaclass=TypeChecker):
    """Class for managing Excel workbooks."""

    def _validate_filename(self, filename: str) -> None:
        """Validate filename.

        Parameters
        ----------
        filename : str
            File path to validate

        Raises
        ------
        ValueError
            If filename is empty or not a string
        """
        if not filename:
            raise ValueError("Filename cannot be empty")
        if not isinstance(filename, str):
            raise ValueError("Filename must be a string")

    def __init__(self, excel: object, filename: str) -> None:
        """Initialize Excel workbook.

        Parameters
        ----------
        excel : object
            Excel application object
        filename : str
            File path for the workbook

        Raises
        ------
        ValueError
            If inputs are invalid or workbook creation fails
        """
        self._validate_filename(filename)
        self.excel = excel
        self.filename = filename
        self.worksheets: dict = {}
        try:
            self.workbook = self.excel.Workbooks.Add()
        except Exception as err:
            raise ValueError(f"Failed to create workbook: {str(err)}") from err

    def add_worksheet(self, name: str) -> ExcelWorksheet:
        """Add a new worksheet to the workbook.

        Parameters
        ----------
        name : str
            Name of the new worksheet

        Returns
        -------
        ExcelWorksheet
            Created worksheet object

        Raises
        ------
        ValueError
            If worksheet name is invalid or creation fails
        """
        if not name:
            raise ValueError("Worksheet name cannot be empty")
        if not isinstance(name, str):
            raise ValueError("Worksheet name must be a string")
        try:
            worksheet = ExcelWorksheet(self.excel, self.workbook, name)
            self.worksheets[name] = worksheet
            return worksheet
        except Exception as err:
            raise ValueError(f"Failed to add worksheet: {str(err)}") from err

    def add_chart(self, name: str, after_sheet: object) -> ExcelChart:
        """Add a new chart to the workbook.

        Parameters
        ----------
        name : str
            Name of the new chart
        after_sheet : object
            Sheet after which to add chart

        Returns
        -------
        ExcelChart
            Created chart object

        Raises
        ------
        ValueError
            If chart name is invalid or creation fails
        """
        if not name:
            raise ValueError("Chart name cannot be empty")
        if not isinstance(name, str):
            raise ValueError("Chart name must be a string")
        try:
            chart = ExcelChart(self.excel, self.workbook, name, after_sheet)
            self.worksheets[name] = chart
            return chart
        except Exception as err:
            raise ValueError(f"Failed to add chart: {str(err)}") from err

    def save(self) -> None:
        """Save the workbook.

        Raises
        ------
        ValueError
            If save operation fails
        """
        try:
            self.workbook.SaveAs(self.filename)
        except Exception as err:
            raise ValueError(f"Failed to save workbook: {str(err)}") from err

    def close(self) -> None:
        """Close the workbook.

        Raises
        ------
        ValueError
            If close operation fails
        """
        try:
            self.worksheets = {}
            self.workbook.Close()
        except Exception as err:
            raise ValueError(f"Failed to close workbook: {str(err)}") from err

    def set_author(self, author: str) -> None:
        """Set workbook author.

        Parameters
        ----------
        author : str
            Author name

        Raises
        ------
        ValueError
            If author is invalid or set operation fails
        """
        if not isinstance(author, str):
            raise ValueError("Author must be a string")
        try:
            self.workbook.Author = author
        except Exception as err:
            raise ValueError(f"Failed to set author: {str(err)}") from err


class ExcelApp(metaclass=TypeChecker):
    """Class for managing Excel application instance."""

    def __init__(self) -> None:
        """Initialize Excel application.

        Raises
        ------
        ValueError
            If application initialization fails
        """
        try:
            self.excel = Dispatch("Excel.Application")
            self.workbooks: list[ExcelWorkbook] = []
            self.set_default_sheet_num(1)
        except Exception as err:
            raise ValueError(f"Failed to initialize Excel application: {str(err)}") from err

    def show(self) -> None:
        """Make Excel application visible.

        Raises
        ------
        ValueError
            If visibility setting fails
        """
        try:
            self.excel.Visible = 1
        except Exception as err:
            raise ValueError(f"Failed to set visibility: {str(err)}") from err

    def hide(self) -> None:
        """Hide Excel application.

        Raises
        ------
        ValueError
            If visibility setting fails
        """
        try:
            self.excel.Visible = 0
        except Exception as err:
            raise ValueError(f"Failed to set visibility: {str(err)}") from err

    def quit(self) -> None:
        """Quit Excel application.

        Raises
        ------
        ValueError
            If quit operation fails
        """
        try:
            for wkb in self.workbooks:
                wkb.close()
            self.excel.Quit()
        except Exception as err:
            raise ValueError(f"Failed to quit Excel application: {str(err)}") from err

    def set_default_sheet_num(self, num_sheets: int) -> None:
        """Set default number of sheets for new workbooks.

        Parameters
        ----------
        num_sheets : int
            Number of sheets

        Raises
        ------
        ValueError
            If number of sheets is invalid or set operation fails
        """
        if not isinstance(num_sheets, int) or num_sheets < 1:
            raise ValueError("Number of sheets must be a positive integer")
        try:
            self.excel.SheetsInNewWorkbook = num_sheets
        except Exception as err:
            raise ValueError(f"Failed to set default sheet number: {str(err)}") from err

    def add_workbook(self, filename: str) -> ExcelWorkbook:
        """Add a new workbook.

        Parameters
        ----------
        filename : str
            File path for the new workbook

        Returns
        -------
        ExcelWorkbook
            Created workbook object

        Raises
        ------
        ValueError
            If filename is invalid or workbook creation fails
        """
        if not filename:
            raise ValueError("Filename cannot be empty")
        if not isinstance(filename, str):
            raise ValueError("Filename must be a string")
        try:
            workbook = ExcelWorkbook(self.excel, filename)
            self.workbooks.append(workbook)
            return workbook
        except Exception as err:
            raise ValueError(f"Failed to add workbook: {str(err)}") from err


class ExcelWriter(metaclass=TypeChecker):
    """Class for creating and formatting Excel spreadsheets.

    References
    ----------
    .. [1] https://gist.github.com/mikepsn/27dd0d768ccede849051#file-excelapp-py-L16
    .. [2] http://code.activestate.com/recipes/528870-class-for-writing-content-to-excel-and-formatting-/
    """

    def _validate_filename(self, filename: str) -> None:
        """Validate filename.

        Parameters
        ----------
        filename : str
            File path to validate

        Raises
        ------
        ValueError
            If filename is empty or not a string
        """
        if not filename:
            raise ValueError("Filename cannot be empty")
        if not isinstance(filename, str):
            raise ValueError("Filename must be a string")

    def _validate_sheet_name(self, sheet_name: str) -> None:
        """Validate sheet name.

        Parameters
        ----------
        sheet_name : str
            Sheet name to validate

        Raises
        ------
        ValueError
            If sheet name is empty or not a string
        """
        if not sheet_name:
            raise ValueError("Sheet name cannot be empty")
        if not isinstance(sheet_name, str):
            raise ValueError("Sheet name must be a string")

    def _validate_cell_position(self, row: int, col: int) -> None:
        """Validate cell row and column positions.

        Parameters
        ----------
        row : int
            Row number
        col : int
            Column number

        Raises
        ------
        ValueError
            If row or column is invalid
        """
        if not isinstance(row, int) or row < 1:
            raise ValueError("Row must be a positive integer")
        if not isinstance(col, int) or col < 1:
            raise ValueError("Column must be a positive integer")

    def __init__(
        self, 
        file_name: str, 
        default_sheet_name: str, 
        make_visible: bool = False
    ) -> None:
        """Initialize Excel writer.

        Parameters
        ----------
        file_name : str
            File path for the Excel file
        default_sheet_name : str
            Name of the default sheet
        make_visible : bool
            Whether to make Excel visible (default: False)

        Raises
        ------
        ValueError
            If inputs are invalid or initialization fails

        References
        ----------
        .. [1] https://gist.github.com/mikepsn/27dd0d768ccede849051#file-excelapp-py-L16
        .. [2] http://code.activestate.com/recipes/528870-class-for-writing-content-to-excel-and-formatting-/
        """
        self._validate_filename(file_name)
        self._validate_sheet_name(default_sheet_name)
        try:
            self.excelapp = Dispatch("Excel.Application")
            self.excelapp.Visible = 1 if make_visible else 0
            self.excelapp.Workbooks.Add()
            self.workbook = self.excelapp.ActiveWorkbook
            self.file_name = file_name
            self.default_sheet = self.excelapp.ActiveSheet
            self.default_sheet.Name = default_sheet_name
        except Exception as err:
            raise ValueError(f"Failed to initialize Excel writer: {str(err)}") from err

    def get_excel_app(self) -> object:
        """Get Excel application object.

        Returns
        -------
        object
            Excel application object

        Raises
        ------
        ValueError
            If retrieval fails
        """
        try:
            return self.excelapp
        except Exception as err:
            raise ValueError(f"Failed to get Excel application: {str(err)}") from err

    def add_sheet_after(self, sheet_name: str, index_or_name: Union[str, int]) -> None:
        """Add new sheet after specified index or name.

        Parameters
        ----------
        sheet_name : str
            Name of the new sheet
        index_or_name : Union[str, int]
            Index (1-based) or name of sheet to add after

        Raises
        ------
        ValueError
            If inputs are invalid or sheet addition fails

        References
        ----------
        .. [1] http://www.functionx.com/vbaexcel/Lesson07.htm
        """
        self._validate_sheet_name(sheet_name)
        if not isinstance(index_or_name, (str, int)):
            raise ValueError("Index or name must be a string or integer")
        try:
            sheets = self.workbook.Sheets
            sheets.Add(None, sheets(index_or_name)).Name = sheet_name
        except Exception as err:
            raise ValueError(f"Failed to add sheet: {str(err)}") from err

    def delete_sheet(self, sheet_name: str) -> None:
        """Delete named sheet.

        Parameters
        ----------
        sheet_name : str
            Name of the sheet to delete

        Raises
        ------
        ValueError
            If sheet name is invalid or delete operation fails

        References
        ----------
        .. [1] http://www.exceltip.com/st/Delete_sheets_without_confirmation_prompts_using_VBA_in_Microsoft_Excel/483.html
        """
        self._validate_sheet_name(sheet_name)
        try:
            sheets = self.workbook.Sheets
            self.excelapp.DisplayAlerts = False
            sheets(sheet_name).Delete()
            self.excelapp.DisplayAlerts = True
        except Exception as err:
            raise ValueError(f"Failed to delete sheet: {str(err)}") from err

    def get_sheet(self, sheet_name: str) -> object:
        """Get sheet by name.

        Parameters
        ----------
        sheet_name : str
            Name of the sheet

        Returns
        -------
        object
            Worksheet object

        Raises
        ------
        ValueError
            If sheet name is invalid or retrieval fails
        """
        self._validate_sheet_name(sheet_name)
        try:
            return self.workbook.Sheets(sheet_name)
        except Exception as err:
            raise ValueError(f"Failed to get sheet: {str(err)}") from err

    def activate_sheet(self, sheet_name: str) -> None:
        """Activate named sheet.

        Parameters
        ----------
        sheet_name : str
            Name of the sheet to activate

        Raises
        ------
        ValueError
            If sheet name is invalid or activation fails
        """
        self._validate_sheet_name(sheet_name)
        try:
            self.workbook.Sheets(sheet_name).Activate()
        except Exception as err:
            raise ValueError(f"Failed to activate sheet: {str(err)}") from err

    def add_to_cell(
        self, 
        row: int, 
        col: int, 
        content: str, 
        sheet: Optional[object] = None
    ) -> None:
        """Add content to cell at row,col location.

        Parameters
        ----------
        row : int
            Row number
        col : int
            Column number
        content : str
            Content to add
        sheet : Optional[object]
            Target sheet (default: default sheet)

        Raises
        ------
        ValueError
            If inputs are invalid or cell write operation fails

        References
        ----------
        .. [1] http://support.microsoft.com/kb/247412
        """
        self._validate_cell_position(row, col)
        if not isinstance(content, str):
            raise ValueError("Content must be a string")
        try:
            target_sheet = sheet if sheet is not None else self.default_sheet
            target_sheet.Cells(row, col).Value = content
        except Exception as err:
            raise ValueError(f"Failed to add content to cell: {str(err)}") from err

    def add_row(
        self, 
        row_i: int, 
        data_tuple: tuple[str, ...], 
        start_col: int = 1, 
        sheet: Optional[object] = None
    ) -> None:
        """Add row of data in a single operation.

        Parameters
        ----------
        row_i : int
            Row number
        data_tuple : tuple[str, ...]
            tuple of data for the row
        start_col : int
            Starting column number (default: 1)
        sheet : Optional[object]
            Target sheet (default: default sheet)

        Raises
        ------
        ValueError
            If inputs are invalid or row write operation fails

        References
        ----------
        .. [1] http://support.microsoft.com/kb/247412
        """
        self._validate_cell_position(row_i, start_col)
        if not isinstance(data_tuple, tuple):
            raise ValueError("Data must be a tuple")
        if not all(isinstance(item, str) for item in data_tuple):
            raise ValueError("All data items must be strings")
        try:
            target_sheet = sheet if sheet is not None else self.default_sheet
            col_n = len(data_tuple)
            last_col = start_col + col_n - 1
            insert_range = self.get_range_by_cells((row_i, start_col), (row_i, last_col), 
                                                   target_sheet)
            insert_range.Value = data_tuple
        except Exception as err:
            raise ValueError(f"Failed to add row: {str(err)}") from err

    def add_multiple_rows(
        self, 
        start_row: int, 
        list_data_tuples: list[tuple[str, ...]], 
        start_col: int = 1, 
        sheet: Optional[object] = None
    ) -> int:
        """Add multiple rows of data at once.

        Parameters
        ----------
        start_row : int
            Starting row number
        list_data_tuples : list[tuple[str, ...]]
            list of tuples containing row data
        start_col : int
            Starting column number (default: 1)
        sheet : Optional[object]
            Target sheet (default: default sheet)

        Returns
        -------
        int
            Next available row number

        Raises
        ------
        ValueError
            If inputs are invalid or rows write operation fails

        References
        ----------
        .. [1] http://support.microsoft.com/kb/247412
        """
        self._validate_cell_position(start_row, start_col)
        if not list_data_tuples:
            raise ValueError("Data tuples list cannot be empty")
        if not all(isinstance(tup, tuple) and all(isinstance(item, str) for item in tup)
                   for tup in list_data_tuples):
            raise ValueError("All data items must be tuples of strings")
        try:
            target_sheet = sheet if sheet is not None else self.default_sheet
            row_n = len(list_data_tuples)
            last_row = start_row + row_n - 1
            col_n = len(list_data_tuples[0])
            last_col = start_col + col_n - 1
            insert_range = \
                self.get_range_by_cells((start_row, start_col), (last_row, last_col), target_sheet)
            insert_range.Value = list_data_tuples
            return last_row + 1
        except Exception as err:
            raise ValueError(f"Failed to add multiple rows: {str(err)}") from err

    def get_range_by_cells(
        self, 
        cell_start: tuple[int, int], 
        cell_end: tuple[int, int], 
        sheet: Optional[object] = None
    ) -> object:
        """Get Excel range by cell coordinates.

        Parameters
        ----------
        cell_start : tuple[int, int]
            Starting cell (row, column)
        cell_end : tuple[int, int]
            Ending cell (row, column)
        sheet : Optional[object]
            Target sheet (default: default sheet)

        Returns
        -------
        object
            Excel range object

        Raises
        ------
        ValueError
            If inputs are invalid or range retrieval fails
        """
        if not all(isinstance(pos, tuple) 
                   and len(pos) == 2 
                   and all(isinstance(n, int) and n >= 1 for n in pos)
                   for pos in (cell_start, cell_end)):
            raise ValueError("Cell coordinates must be tuples of positive integers")
        try:
            target_sheet = sheet if sheet is not None else self.default_sheet
            return target_sheet.Range(
                target_sheet.Cells(cell_start[0], cell_start[1]),
                target_sheet.Cells(cell_end[0], cell_end[1])
            )
        except Exception as err:
            raise ValueError(f"Failed to get range: {str(err)}") from err

    def fit_cols(
        self, 
        col_start: int, 
        col_sup: int, 
        sheet: Optional[object] = None
    ) -> None:
        """Fit columns to content width.

        Parameters
        ----------
        col_start : int
            Starting column number
        col_sup : int
            Ending column number
        sheet : Optional[object]
            Target sheet (default: default sheet)

        Raises
        ------
        ValueError
            If inputs are invalid or fit operation fails
        """
        if not isinstance(col_start, int) or col_start < 1:
            raise ValueError("Start column must be a positive integer")
        if not isinstance(col_sup, int) or col_sup < col_start:
            raise ValueError("End column must be a positive integer greater than or equal to "
                             + "start column")
        try:
            target_sheet = sheet if sheet is not None else self.default_sheet
            for col_n in range(col_start, col_sup + 1):
                self.fit_col(col_n, target_sheet)
        except Exception as err:
            raise ValueError(f"Failed to fit columns: {str(err)}") from err

    def fit_col(
        self, 
        col_n: int, 
        sheet: Optional[object] = None
    ) -> None:
        """Fit single column to content width.

        Parameters
        ----------
        col_n : int
            Column number
        sheet : Optional[object]
            Target sheet (default: default sheet)

        Raises
        ------
        ValueError
            If inputs are invalid or fit operation fails
        """
        if not isinstance(col_n, int) or col_n < 1:
            raise ValueError("Column number must be a positive integer")
        try:
            target_sheet = sheet if sheet is not None else self.default_sheet
            target_sheet\
                .Range(target_sheet.Cells(1, col_n), target_sheet.Cells(1, col_n))\
                .EntireColumn.AutoFit()
        except Exception as err:
            raise ValueError(f"Failed to fit column: {str(err)}") from err

    def set_col_width(
        self, 
        col_n: int, 
        width: float, 
        sheet: Optional[object] = None
    ) -> None:
        """Set column width.

        Parameters
        ----------
        col_n : int
            Column number
        width : float
            Column width
        sheet : Optional[object]
            Target sheet (default: default sheet)

        Raises
        ------
        ValueError
            If inputs are invalid or width setting fails
        """
        if not isinstance(col_n, int) or col_n < 1:
            raise ValueError("Column number must be a positive integer")
        if not isinstance(width, (int, float)) or width <= 0:
            raise ValueError("Width must be a positive number")
        try:
            target_sheet = sheet if sheet is not None else self.default_sheet
            target_sheet\
                .Range(target_sheet.Cells(1, col_n), target_sheet.Cells(1, col_n))\
                .ColumnWidth = width
        except Exception as err:
            raise ValueError(f"Failed to set column width: {str(err)}") from err

    def format_range(
        self, 
        range_obj: object, 
        style: TypeStyleXLObject
    ) -> None:
        """Apply formatting to a range.

        Parameters
        ----------
        range_obj : object
            Excel range object
        style : TypeStyleXLObject
            Style to apply

        Raises
        ------
        ValueError
            If style is invalid or formatting fails

        References
        ----------
        .. [1] http://www.cpearson.com/excel/colors.htm
        """
        if style not in [STYLE_HEADING1, STYLE_HEADING2, STYLE_BORDER_BOTTOM, 
                         STYLE_GREY_CELL, STYLE_PALE_YELLOW_CELL, STYLE_ITALICS]:
            raise ValueError(f"Style '{style}' has not been defined")
        try:
            if style == STYLE_HEADING1:
                range_obj.Font.Bold = True
                range_obj.Font.Name = "Arial"
                range_obj.Font.Size = 12
            elif style == STYLE_HEADING2:
                range_obj.Font.Bold = True
                range_obj.Font.Name = "Arial"
                range_obj.Font.Size = 10.5
            elif style == STYLE_BORDER_BOTTOM:
                range_obj.Borders(XL_CONST_EDGE_BOTTOM).LineStyle = XL_CONST_CONTINUOUS
                range_obj.Borders(XL_CONST_EDGE_BOTTOM).Weight = XL_CONST_THIN
                range_obj.Borders(XL_CONST_EDGE_BOTTOM).ColorIndex = XL_CONST_AUTOMATIC
            elif style == STYLE_GREY_CELL:
                self.reset_color_pallet(1, RGB_PALE_GREY)
                range_obj.Interior.ColorIndex = 1
                range_obj.Interior.Pattern = XL_CONST_SOLID
            elif style == STYLE_PALE_YELLOW_CELL:
                self.reset_color_pallet(1, RGB_PALE_YELLOW)
                range_obj.Interior.ColorIndex = 1
                range_obj.Interior.Pattern = XL_CONST_SOLID
            elif style == STYLE_ITALICS:
                range_obj.Font.Italic = True
        except Exception as err:
            raise ValueError(f"Failed to format range: {str(err)}") from err

    def reset_color_pallet(
        self, 
        color_index: int, 
        color: int
    ) -> None:
        """Reset color in Excel palette.

        Parameters
        ----------
        color_index : int
            Color index (1-56)
        color : int
            RGB color value

        Raises
        ------
        ValueError
            If inputs are invalid or palette reset fails

        References
        ----------
        .. [1] http://www.cpearson.com/excel/colors.htm
        """
        if not isinstance(color_index, int) or color_index < 1 or color_index > 56:
            raise ValueError("Color index must be between 1 and 56")
        if not isinstance(color, int) or color < 0:
            raise ValueError("Color value must be a non-negative integer")
        try:
            colors_tup = self.workbook.Colors
            colors_list = list(colors_tup)
            colors_list[color_index - 1] = color
            self.workbook.Colors = tuple(colors_list)
        except Exception as err:
            raise ValueError(f"Failed to reset color palette: {str(err)}") from err

    def merge_range(self, range_obj: object) -> None:
        """Merge specified range.

        Parameters
        ----------
        range_obj : object
            Excel range object to merge

        Raises
        ------
        ValueError
            If merge operation fails
        """
        try:
            range_obj.Merge()
        except Exception as err:
            raise ValueError(f"Failed to merge range: {str(err)}") from err

    def save(self) -> None:
        """Save spreadsheet.

        Raises
        ------
        ValueError
            If save operation fails
        """
        try:
            if os.path.exists(self.file_name):
                os.remove(self.file_name)
            self.workbook.SaveAs(self.file_name)
        except Exception as err:
            raise ValueError(f"Failed to save spreadsheet: {str(err)}") from err

    def close(self) -> None:
        """Close spreadsheet resources.

        Raises
        ------
        ValueError
            If close operation fails

        References
        ----------
        .. [1] http://www.functionx.com/vbaexcel/Lesson07.htm
        """
        try:
            self.workbook.Saved = 0
            self.workbook.Close(SaveChanges=0)
            self.excelapp.Visible = 0
            self.excelapp.Quit()
            del self.excelapp
        except Exception as err:
            raise ValueError(f"Failed to close spreadsheet: {str(err)}") from err