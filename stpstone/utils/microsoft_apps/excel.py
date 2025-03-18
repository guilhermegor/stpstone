### BIBLIOTECA COM FUNÇÕES PARA USAR COM O EXCEL ###
"""
### fontes:  ###
https://gist.github.com/mikepsn/27dd0d768ccede849051#file-excelapp-py-L16
http://code.activestate.com/recipes/528870-class-for-writing-content-to-excel-and-formatting-/
"""
from __future__ import unicode_literals
import os
import io
import pandas as pd
from win32com.client import Dispatch, constants
from xlwt import Workbook
from ctypes import windll
from stpstone.utils.parsers.folders import DirFilesManagement


STYLE_HEADING1 = "style_heading1"
STYLE_HEADING2 = "style_heading2"
STYLE_BORDER_BOTTOM = "style_border_bottom"
STYLE_GREY_CELL = "style_grey_cell"
STYLE_PALE_YELLOW_CELL = "style_pale_yellow_cell"
STYLE_ITALICS = "style_italics"

# these are the constant values in one particular version of EXCEL - if having problems,
# check your own
XL_CONST_EDGE_LEFT = 7
XL_CONST_EDGE_BOTTOM = 9
XL_CONST_CONTINUOUS = 1
XL_CONST_AUTOMATIC = -4105
XL_CONST_THIN = 2
XL_CONST_GRAY16 = 17
XL_CONST_SOLID = 1

# Debug.Print RGB(230,230,230) in Excel Immediate window
RGB_PALE_GREY = 15132390
RGB_PALE_YELLOW = 13565951  # RGB(255,255,206)


class DealingExcel:

    def save_as(self, active_wb, nome_comp_arq):
        """
        DOCSTRING: SALVAR COMO EXCEL DE INTERESSE
        INPUTS: WORKBOOK ATIVO E NOME COMPLETO DO ARQUIVO XLS
        OUTPUTS: -
        """
        xlapp = Dispatch('Excel.Application')
        xlapp.Visible = 0
        wb = xlapp.Workbooks.Open(active_wb)
        wb.SaveAs(nome_comp_arq)
        wb.Close(True)

    def save_as_active_wb(self, filename, filename_as):
        """
        DOCSTRING: SAVE AS ACTIVE WORKBOOK
        INPUTS: WORKBOOK NAME AND NAME TO SAVE AS
        OUTPUTS: STATUS OF ACCOMPLISHMENT
        """
        xlapp = Dispatch('Excel.Application')
        xlapp.Visible = 0
        wb = xlapp.Workbooks.Open(filename)
        wb.SaveAs(filename_as)
        wb.Close(True)

        return DirFilesManagement().object_exists(filename_as)

    def open_xl(self, nome_comp_arq):
        """
        DOCSTRING: ABRIR EXCEL DE INTERESSE
        INPUTS: NOME DO ARQUIVO DE INTERESSE
        OUTPUTS: EXCEL APP E PASTA DE TRABALHO ABERTA
        """
        xlapp = Dispatch('Excel.Application')
        xlapp.Visible = 0
        wb = xlapp.Workbooks.Open(nome_comp_arq)

        return xlapp, wb

    def close_wb(self, active_wb, save_orn=True):
        """
        DOCSTRING: CLOSE ACTIVE WORKBOOK
        INPUTS: ACTIVE WORKBOOK AND WHETER SAVE IT OR NOT (BOOLEAN)
        OUTPUTS: -
        """
        active_wb.Close(save_orn)

    def delete_sheet(self, sheet_name, excel_app, active_wb):
        """
        DOCSTRING: DELETE A SHEET FROM THE WORKBOOK
        INPUTS: SHEET NAME, XP APP AND WORBOOK
        """
        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        sheets = active_wb.Sheets
        excel_app.DisplayAlerts = False
        sheets(sheet_name).Delete()
        excel_app.DisplayAlerts = True

    def paste_values_column(self, nome_plan, str_range, excel_app, active_wb):
        """
        DOCSTRING: COLAR COMO VALOR COLUNA DE INTERESSE
        INPUTS: COLUNA COMO STR
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        excel_app.Range(str_range).Copy()
        excel_app.Range(str_range).PasteSpecial(Paste=-4163)
        DealingExcel().clearclipboard()

    def color_range_w_rule(self, nome_plan, str_range, matching_value, cor_pintar_intervalo,
                           excel_app, active_wb):
        """
        DOCSTRING: COLORIR TODA A LINHA
        INPUTS: NOME DA PLAN, COLUNA COMO STR, XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # Selecionar apenas números no intervalo
        for cel in excel_app.ActiveSheet.Range(str_range).SpecialCells(2):
            if str(cel) == matching_value:
                cel.EntireRow.Interior.Pattern = 1
                cel.EntireRow.Interior.PatternColorIndex = -4105
                cel.EntireRow.Interior.Color = cor_pintar_intervalo
                cel.EntireRow.Interior.TintAndShade = 0
                cel.EntireRow.Interior.PatternTintAndShade = 0

    def autofit_range_columns(self, nome_plan, str_range, excel_app, active_wb):
        """
        DOCSTRING: COLAR COMO VALOR COLUNA DE INTERESSE
        INPUTS: NOME DA PLAN, COLUNA COMO STR, XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # Autofit column in active sheet
        excel_app.ActiveSheet.Columns(str_range).Columns.AutoFit()

    def delete_entire_column(self, nome_plan, str_range, excel_app, active_wb):
        """
        DOCSTRING: DELETAR COLUNA DE UMA PLANILHA EXCEL
        INPUTS: NOME COMPLETO DO ARQUIVO, PLAN DE INTERESSE, RANGE DE INTERESSE (COLUNA) ,
        XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # Delete column of interest
        excel_app.ActiveSheet.Range(str_range).Columns.Delete()

    def delete_cells_w_specific_data(self, nome_plan, str_range, value_to_delete,
                                     excel_app, active_wb):
        """
        DOCSTRING: DELETAR VALORES DE UMA COLUNA CASO COINCIDAM COM UM VALOR ESPECÍFICO
        INPUTS: RANGE DE INTERESSE (COLUNA) , NOME PLANILHA, VALOR DE INTERESSE A SER DELETADO,
            XL APP E WORKBOOK ATIVO
        """
        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # deletar dado de interesse em range
        for cel in excel_app.ActiveSheet.Range(str_range).SpecialCells(2):
            if str(cel) == value_to_delete:
                excel_app.ActiveSheet.Cells(
                    cel.row, cel.column + 1).value = 'DELETAR'

        excel_app.Worksheets(nome_plan).Cells.Find('DELETAR').EntireColumn.SpecialCells(
            2).EntireRow.Delete

    def replacing_all_matching_str_column(self, nome_plan, str_range, value_to_replace,
                                          replace_value, excel_app, active_wb):
        """
        DOCSTRING: SUBSTITUIR TODOS OS VALORES NUMÉRICOS EM UMA COLUNA DE UMA ARQUIVO EXCEL
        INPUTS: NOME COMPLETO DO ARQUIVO, PLAN DE INTERESSE E RANGE DE INTERESSE (COLUNA)
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # Selecionar apenas números no intervalo
        for cel in excel_app.ActiveSheet.Range(str_range).SpecialCells(2):
            if str(cel) == value_to_replace:
                cel.value = replace_value

    def create_new_column(self, nome_plan, str_range, excel_app, active_wb):
        """
        DOCSTRING: CRIAR COLUNA EM UMA PLANILHA EXCEL
        INPUTS: NOME COMPLETO DO ARQUIVO, PLAN DE INTERESSE, RANGE DE INTERESSE (COLUNA) ANTES DA QUAL
        SERÁ CRIADA A COLUNA, XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # Delete column of interest
        excel_app.ActiveSheet.Columns(str_range).Insert()

    def text_to_columns(self, nome_plan, str_range, excel_app, active_wb):
        """
        DOCSTRING: CRIAR COLUNA EM UMA PLANILHA EXCEL
        INPUTS: NOME COMPLETO DO ARQUIVO, PLAN DE INTERESSE, RANGE DE INTERESSE (COLUNA) ANTES DA QUAL
        SERÁ CRIADA A COLUNA, XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # Delete column of interest
        excel_app.ActiveSheet.Range(str_range).TextToColumns()

    def conf_title(self, nome_plan, str_range, excel_app, active_wb):
        """
        DOCSTRING: CONFIGURE
        INPUTS: NOME COMPLETO DO ARQUIVO, PLAN DE INTERESSE, RANGE DE INTERESSE (COLUNA) ANTES DA QUAL
        SERÁ CRIADA A COLUNA, XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # Changing borders
        excel_app.Range(str_range).Borders(-4131).LineStyle = -4142
        excel_app.Range(str_range).Borders(-4152).LineStyle = -4142
        excel_app.Range(str_range).Borders(-4160).LineStyle = -4142
        excel_app.Range(str_range).Borders(-4107).LineStyle = -4142
        excel_app.Range(str_range).Borders(5).LineStyle = -4142
        excel_app.Range(str_range).Borders(6).LineStyle = -4142

        # Bolding text font
        excel_app.Range(str_range).Font.Bold = True

    def copy_column_paste_in_other(self, nome_plan, str_range_a, str_range_b, excel_app, active_wb):
        """
        DOCSTRING: CRIAR COLUNA EM UMA PLANILHA EXCEL
        INPUTS: NOME COMPLETO DO ARQUIVO, PLAN DE INTERESSE, RANGE DE INTERESSE (COLUNA) ANTES DA QUAL
        SERÁ CRIADA A COLUNA, XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # copy & paste column
        excel_app.Range(str_range_a).Copy()
        excel_app.Range(str_range_b).PasteSpecial(-4104)
        DealingExcel().clearclipboard()

    def move_column(self, nome_plan, str_range_a, str_range_b, excel_app, active_wb):
        """
        DOCSTRING: CRIAR COLUNA EM UMA PLANILHA EXCEL
        INPUTS: NOME COMPLETO DO ARQUIVO, PLAN DE INTERESSE, RANGE DE INTERESSE (COLUNA) ANTES DA QUAL
        SERÁ CRIADA A COLUNA, XL APP E WORKBOOK ATIVO
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # copy & paste column
        excel_app.Columns(str_range_a).Cut()
        excel_app.Columns(str_range_b).Insert()
        DealingExcel().clearclipboard()

    def attr_value_cel(self, nome_plan, str_range, value_inter, excel_app, active_wb):
        """
        DOCSTRING: CHANGE A CEL VALUE
        INPUTS: NOME PLAN INTERESSE, RANGE (CEL), NEW VALUE, XL APP E ACTIVEWORKBOOK
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # attribute value
        excel_app.Range(str_range).Value = value_inter

    def number_format(self, nome_plan, str_range, format_inter, excel_app, active_wb):
        """
        DOCSTRING: CHANGE A CEL VALUE
        INPUTS: NOME PLAN INTERESSE, RANGE (CEL), NEW VALUE, XL APP E ACTIVEWORKBOOK
        OUTPUTS: -
        """

        # activate workbook
        active_wb.Activate()

        # Activate sheet of interest
        excel_app.Worksheets(nome_plan).Activate()

        # attribute value
        excel_app.Range(str_range).NumberFormat = format_inter

    def clearclipboard(self):
        if windll.user32.OpenClipboard(None):
            windll.user32.EmptyClipboard()
            windll.user32.CloseClipboard()

    def restore_corrupted_xl(self, filename_orig, filename_output):
        # Opening the file using 'utf-16'/"iso-8859-1" encoding
        file1 = io.open(filename_orig, "r", encoding='utf-16')
        data = file1.readlines()

        # Creating a workbook object
        xldoc = Workbook()
        # Adding a sheet to the workbook object
        sheet = xldoc.add_sheet("Sheet1", cell_overwrite_ok=True)
        # Iterating and saving the data to sheet
        for i, row in enumerate(data):
            # Two things are done here
            # Removeing the '\n' which comes while reading the file using io.open
            # Getting the values after splitting using '\t'
            for j, val in enumerate(row.replace('\n', '').split('\t')):
                sheet.write(i, j, val)

        # Saving the file as an excel file
        xldoc.save(filename_output)

        return DirFilesManagement().object_exists(filename_output)


# excelchart
# creates a microsoft excel chart given a data range
# and whole bunch of other parameters


class ExcelChart:
    def __init__(self, excel, workbook, chartname, afterSheet):
        self.chartname = chartname
        self.excel = excel
        self.workbook = workbook
        self.chartname = chartname
        self.afterSheet = afterSheet

    def SetTitle(self, chartTitle):
        self.chartTitle = chartTitle

    def SetType(self, chartType):
        self.chartType = chartType

    def SetSource(self, chartSource):
        self.chartSource = chartSource

    def SetPlotBy(self, plotBy):
        self.plotBy = plotBy

    def SetCategoryLabels(self, numCategoryLabels):
        self.numCategoryLabels = numCategoryLabels

    def SetSeriesLabels(self, numSeriesLabels):
        self.numSeriesLabels = numSeriesLabels

    def SetCategoryTitle(self, categoryTitle):
        self.categoryTitle = categoryTitle

    def SetValueTitle(self, valueTitle):
        self.valueTitle = valueTitle

    def CreateChart(self):
        self.chart = self.workbook.Charts.Add(After=self.afterSheet)
        self.chart.ChartWizard(Gallery=constants.xlColumn,
                               CategoryLabels=1,
                               SeriesLabels=1,
                               CategoryTitle=self.categoryTitle,
                               ValueTitle=self.valueTitle,
                               PlotBy=self.plotBy,
                               Title=self.chartTitle)
        self.chart.SetSourceData(self.chartSource, self.plotBy)
        self.chart.HasAxis = (constants.xlCategory, constants.xlPrimary)
        self.chart.Axes(constants.xlCategory).HasTitle = 1
        self.chart.Axes(
            constants.xlCategory).AxisTitle.Text = self.categoryTitle
        self.chart.Axes(constants.xlValue).HasTitle = 1
        self.chart.Axes(constants.xlValue).AxisTitle.Text = self.valueTitle
        self.chart.Axes(
            constants.xlValue).AxisTitle.Orientation = constants.xlUpward
        self.chart.PlotBy = self.plotBy
        self.chart.Name = self.chartname
        self.chart.HasTitle = 1
        self.chart.ChartTitle.Text = self.chartTitle
        self.chart.HasDataTable = 0
        self.chart.ChartType = self.chartType

    def SetLegendPosition(self, legendPosition):
        self.chart.Legend.Position = legendPosition

    def PlotByColumns(self):
        self.chart.PlotBy = constants.xlColumns

    def PlotByRows(self):
        self.chart.PlotBy = constants.xlRows

    def SetCategoryAxisRange(self, minValue, maxValue):
        self.chart.Axes(constants.xlCategory).MinimumScale = minValue
        self.chart.Axes(constants.xlCategory).MaximumScale = maxValue

    def SetValueAxisRange(self, minValue, maxValue):
        self.chart.Axes(constants.xlValue).MinimumScale = minValue
        self.chart.Axes(constants.xlValue).MaximumScale = maxValue

    def ApplyDataLabels(self, dataLabelType):
        self.chart.ApplyDataLabels(dataLabelType)

    def SetBorderLineStyle(self, lineStyle):
        self.chart.PlotArea.Border.LineStyle = lineStyle

    def SetInteriorStyle(self, interiorStyle):
        self.chart.PlotArea.Interior.Pattern = interiorStyle

# excelworksheet
# creates an excel worksheet


class ExcelWorksheet:
    def __init__(self, excel, workbook, sheetname):
        self.sheetname = sheetname
        self.excel = excel
        self.workbook = workbook
        self.worksheet = self.workbook.Worksheets.Add()
        self.worksheet.Name = sheetname

    def Activate(self):
        self.worksheet.Activate()

    def SetCell(self, row, col, value):
        self.worksheet.Cells(row, col).Value = value

    def GetCell(self, row, col):
        return self.worksheet.Cells(row, col).Value

    def SetFont(self, row, col, font, size):
        self.worksheet.Cells(row, col).Font.Name = font
        self.worksheet.Cells(row, col).Font.Size = size

    def GetFont(self, row, col):
        font = self.worksheet.Cells(row, col).Font.Name
        size = self.worksheet.Cells(row, col).Font.Size
        return (font, size)

# excelWorkbook
# creates an Excel Workbook


class ExcelWorkbook:
    def __init__(self, excel, filename):
        self.filename = filename
        self.excel = excel
        self.workbook = self.excel.Workbooks.Add()
        self.worksheets = {}

    def addworksheet(self, name):
        worksheet = ExcelWorksheet(self.excel, self.workbook, name)
        self.worksheets[name] = worksheet
        return worksheet

    def addchart(self, name, afterSheet):
        chart = ExcelChart(self.excel, self.workbook, name, afterSheet)
        self.worksheets[name] = chart
        return chart

    def save(self):
        self.workbook.SaveAs(self.filename)

    def close(self):
        self.worksheets = {}
        self.workbook.Close()

    def setauthor(self, author):
        self.workbook.Author = author

# excelapp
# encapsulates an Excel Application


class ExcelApp:
    def __init__(self):
        self.excel = Dispatch("Excel.Application")
        self.workbooks = []
        self.SetDefaultSheetNum(1)

    def Show(self):
        self.excel.Visible = 1

    def Hide(self):
        self.excel.Visible = 0

    def Quit(self):
        for wkb in self.workbooks:
            wkb.Close()
        self.excel.Quit()

    def SetDefaultSheetNum(self, numSheets):
        self.excel.SheetsInNewWorkbook = numSheets

    def AddWorkbook(self, filename):
        workbook = ExcelWorkbook(self.excel, filename)
        self.workbooks.append(workbook)
        return workbook


class ExcelWriter(object):
    """
    Excel class for creating spreadsheets - esp writing data and formatting them
    Based in part on #http://snippets.dzone.com/posts/show/2036,
    and http://www.markcarter.me.uk/computing/python/excel.html
    """

    def __init__(self, file_name, default_sheet_name, make_visible=False):
        """Open spreadsheet"""
        self.excelapp = Dispatch("Excel.Application")
        if make_visible:
            self.excelapp.Visible = 1  # fun to watch!
        self.excelapp.Workbooks.Add()
        self.workbook = self.excelapp.ActiveWorkbook
        self.file_name = file_name
        self.default_sheet = self.excelapp.ActiveSheet
        self.default_sheet.Name = default_sheet_name

    def getExcelApp(self):
        """Get Excel App for use"""
        return self.excelapp

    def addSheetAfter(self, sheet_name, index_or_name):
        """
        Add new sheet to workbook after index_or_name (indexing starts at 1).
        """
        sheets = self.workbook.Sheets
        # Sheets.Add(Before, After, Count, Type) - http://www.functionx.com/vbaexcel/Lesson07.htm
        sheets.Add(None, sheets(index_or_name)).Name = sheet_name

    def deleteSheet(self, sheet_name):
        """Delete named sheet"""
        # http://www.exceltip.com/st/Delete_sheets_without_confirmation_prompts_using_VBA_in_Microsoft_Excel/483.html
        sheets = self.workbook.Sheets
        self.excelapp.DisplayAlerts = False
        sheets(sheet_name).Delete()
        self.excelapp.DisplayAlerts = True

    def getSheet(self, sheet_name):
        """
        Get sheet by name.
        """
        return self.workbook.Sheets(sheet_name)

    def activateSheet(self, sheet_name):
        """
        Activate named sheet.
        """
        sheets = self.workbook.Sheets
        # http://mail.python.org/pipermail/python-win32/2002-February/000249.html
        sheets(sheet_name).Activate()

    def add2cell(self, row, col, content, sheet=None):
        """
        Add content to cell at row,col location.
        NB only recommended for small amounts of data http://support.microsoft.com/kb/247412.
        """
        if sheet is None:
            sheet = self.default_sheet
        sheet.Cells(row, col).Value = content

    def addRow(self, row_i, data_tuple, start_col=1, sheet=None):
        """
        Add row in a single operation.  Takes a tuple per row.
        Much more efficient than cell by cell. http://support.microsoft.com/kb/247412.
        """
        if sheet is None:
            sheet = self.default_sheet
        col_n = len(data_tuple)
        last_col = start_col + col_n - 1
        insert_range = self.getRangeByCells(
            (row_i, start_col), (row_i, last_col), sheet)
        insert_range.Value = data_tuple

    def addMultipleRows(self, start_row, list_data_tuples, start_col=1, sheet=None):
        """
        Adds data multiple rows at a time, not cell by cell. Takes list of tuples
        e.g. cursor.fetchall() after running a query
        One tuple per row.
        Much more efficient than cell by cell or row by row.
        http://support.microsoft.com/kb/247412.
        Returns next available row.
        """
        if sheet is None:
            sheet = self.default_sheet
        row_n = len(list_data_tuples)
        last_row = start_row + row_n - 1
        col_n = len(list_data_tuples[0])
        last_col = start_col + col_n - 1
        insert_range = self.getRangeByCells(
            (start_row, start_col), (last_row, last_col), sheet)
        insert_range.Value = list_data_tuples
        next_available_row = last_row + 1

        return next_available_row

    def getRangeByCells(self, cell_start_row, cell_start_col, cell_sup_row, cell_sup_col,
                        sheet=None):
        """Get a range defined by cell start and cell end e.g. (1,1) A1 and (7,2) B7"""
        if sheet is None:
            sheet = self.default_sheet

        return sheet.Range(sheet.Cells(cell_start_row, cell_start_col),
                           sheet.Cells(cell_sup_row, cell_sup_col))

    def fitCols(self, col_start, col_sup, sheet=None):
        """
        Fit colums to contents.
        """
        if sheet is None:
            sheet = self.default_sheet
        col_n = col_start
        while col_n <= col_sup:
            self.fitCol(col_n, sheet)
            col_n = col_n + 1

    def fitCol(self, col_n, sheet=None):
        """
        Fit column to contents.
        """
        if sheet is None:
            sheet = self.default_sheet
        sheet.Range(sheet.Cells(1, col_n), sheet.Cells(
            1, col_n)).EntireColumn.AutoFit()

    def setColWidth(self, col_n, width, sheet=None):
        """
        Set column width.
        """
        if sheet is None:
            sheet = self.default_sheet
        sheet.Range(sheet.Cells(1, col_n), sheet.Cells(
            1, col_n)).ColumnWidth = width

    def formatRange(self, range, style):
        """
        Add formatting to a cell/group of cells.
        To get methods etc record a macro in EXCEL and look at it.
        To get the value of Excel Constants such as xlEdgeLeft (7) or xlThin (2)
        type e.g. Debug.Print xlEdgeLeft in the Immediate window of the VBA editor and press enter.
        http://www.ureader.com/message/33389340.aspx
        For changing the pallete of 56 colours ref: http://www.cpearson.com/excel/colors.htm
        """
        if style == STYLE_HEADING1:
            range.Font.Bold = True
            range.Font.Name = "Arial"
            range.Font.Size = 12
        elif style == STYLE_HEADING2:
            range.Font.Bold = True
            range.Font.Name = "Arial"
            range.Font.Size = 10.5
        elif style == STYLE_BORDER_BOTTOM:
            range.Borders(XL_CONST_EDGE_BOTTOM).LineStyle = XL_CONST_CONTINUOUS
            range.Borders(XL_CONST_EDGE_BOTTOM).Weight = XL_CONST_THIN
            range.Borders(XL_CONST_EDGE_BOTTOM).ColorIndex = XL_CONST_AUTOMATIC
        elif style == STYLE_GREY_CELL:
            self.resetColorPallet(1, RGB_PALE_GREY)
            range.Interior.ColorIndex = 1
            range.Interior.Pattern = XL_CONST_SOLID
        elif style == STYLE_PALE_YELLOW_CELL:
            self.resetColorPallet(1, RGB_PALE_YELLOW)
            range.Interior.ColorIndex = 1
            range.Interior.Pattern = XL_CONST_SOLID
        elif style == STYLE_ITALICS:
            range.Font.Italic = True
        else:
            raise Exception("Style '%s' has not been defined" % style)

    def resetColorPallet(self, color_index, color):
        """
        Reset indexed color in pallet (limited to 1-56).
        Get color values by Debug.Print RGB(230,230,230) in Excel Immediate window
        """
        if color_index < 1 or color_index > 56:
            raise Exception(
                "Only indexes between 1 and 56 are available in the Excel color pallet.")
        colors_tup = self.workbook.Colors  # immutable of course
        colors_list = list(colors_tup)
        # zero-based in list but not Excel pallet
        colors_list[color_index - 1] = RGB_PALE_GREY
        new_colors_tup = tuple(colors_list)
        self.workbook.Colors = new_colors_tup

    def mergeRange(self, range):
        """Merge range"""
        range.Merge()

    def save(self):
        """Save spreadsheet as filename - wipes if existing"""
        if os.path.exists(self.file_name):
            os.remove(self.file_name)
        self.workbook.SaveAs(self.file_name)

    def close(self):
        """Close spreadsheet resources"""
        self.workbook.Saved = 0  # p.248 Using VBA 5
        self.workbook.Close(SaveChanges=0)  # to avoid prompt
        self.excelapp.Quit()
        # must make Visible=0 before del self.excelapp or EXCEL.EXE remains in memory.
        self.excelapp.Visible = 0
        del self.excelapp
