from __future__ import annotations

import math

import numpy as np
import pandas as pd

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import xlsxwriter

def _handle_nans(worksheet, row, col, number, format=None):
    """handle nan values and convert float to numpy.float64"""

    # write NaN as NA
    if np.isnan(number):
        return worksheet.write_formula(row, col, '=NA()', format, '#N/A')

    # set decimal precision
    number = math.ceil(number * 1e10) / 1e10

    return worksheet.write_number(row, col, number, format)

def add_frame(name: str, frame: pd.DataFrame, 
        workbook: xlsxwriter.Workbook):
    """create worksheet from frame"""

    # add formats
    f1 = workbook.add_format({"bold": True})

    # add worksheet and nan handler
    worksheet = workbook.add_worksheet(str(name))
    worksheet.add_write_handler(float, _handle_nans)

    # write header names
    worksheet.write(0, 0, "STUDY", f1)
    worksheet.write(1, 0, "SCENARIO", f1)
    worksheet.write(2, 0, "REGION", f1)
    worksheet.write(3, 0, "YEAR", f1)

    # set offsets and freeze panes
    skiprows, skipcolumns = 5, 2
    worksheet.freeze_panes(skiprows, skipcolumns)

    # write and format columns
    for col_num, col_data in enumerate(frame.columns.values):

        # respect offset
        col_num += skipcolumns

        # write column header values
        worksheet.write(0, col_num, col_data[0], f1)
        worksheet.write(1, col_num, col_data[1], f1)
        worksheet.write(2, col_num, col_data[2], f1)
        worksheet.write(3, col_num, col_data[3], f1)

        # set column width
        worksheet.set_column(col_num, col_num, 18)

    # format index column
    worksheet.write(4, 0, 'KEY', f1)
    worksheet.write(4, 1, 'UNIT', f1)

    # set column width
    worksheet.set_column(0, 0, 85)

    # write index values
    for row_num, row_data in enumerate(frame.index.values):
        worksheet.write(row_num + skiprows, 0, row_data[0])
        worksheet.write(row_num + skiprows, 1, row_data[1])     

    # write cell values in numeric format    
    for row_num, row_data in enumerate(frame.values):
        for col_num, col_data in enumerate(row_data):
            worksheet.write(row_num + skiprows, 
                    col_num + skipcolumns, col_data)

    return worksheet

def add_series(name: str, series: pd.Series,
    workbook: xlsxwriter.Workbook):

    # add formats
    f1 = workbook.add_format({"bold": True})

    # add worksheet and nan handler
    worksheet = workbook.add_worksheet(str(name))
    worksheet.add_write_handler(float, _handle_nans)

    # set offset and freeze panes
    skiprows, skipcolumns = 1, len(series.index.names)
    worksheet.freeze_panes(skiprows, skipcolumns)

    # write index names
    for nr, level in enumerate(series.index.names):
        worksheet.write(0, nr, level, f1)

    # write header
    worksheet.write(0, skipcolumns, series.name, f1)

    if isinstance(series.index, pd.MultiIndex):

        # write index values for multiindex
        for row_num, row_data in enumerate(series.index.values):
            for col_num, cell_data in enumerate(row_data):
                worksheet.write(row_num + skiprows, col_num, cell_data)

    else:

        # write index values for regular index        
        for row_num, row_data in enumerate(series.index.values):        
            worksheet.write(row_num + skiprows, 0, row_data)
    
    # write cell values
    for row_num, cell_data in enumerate(series.values):
        worksheet.write(row_num + skiprows, skipcolumns, cell_data)

    return worksheet
