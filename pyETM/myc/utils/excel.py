from __future__ import annotations

import math

import numpy as np
import pandas as pd

from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from xlsxwriter.workbook import Workbook
    from xlsxwriter.worksheet import Worksheet
    from xlsxwriter.format import Format

def _handle_nans(worksheet: Worksheet, 
    row: int, col: int, number: float, format=None) -> Literal[-1, 0]:
    """handle nan values and convert float to numpy.float64"""

    # write NaN as NA
    if np.isnan(number):
        return worksheet.write_formula(row, col, '=NA()', format, '#N/A')

    # set decimal precision
    number = math.ceil(number * 1e10) / 1e10

    return worksheet.write_number(row, col, number, format)

def _has_names(index: pd.Index | pd.MultiIndex) -> bool:
    """helper to check if index level(s) are named"""
    return len(list(index.names)) != list(index.names).count(None)

def _set_column_width(worksheet: Worksheet, 
    columns: pd.Index | pd.MultiIndex, offset: int,
    column_width: int | list | None = None) -> None:
    """set header column widths in worksheet"""

    # individual value for all header columns
    if isinstance(column_width, list):

        # check for valid list        
        if len(column_width) != len(columns):
            raise ValueError("column widths do not match number of columns")

        # set column widths individually
        for col_num, col_width in enumerate(column_width):
            worksheet.set_column(col_num + offset, 
                col_num + offset, col_width)

    # single value for all header columns
    if isinstance(column_width, int):
        worksheet.set_column(offset, len(columns) + offset, column_width)

def _set_index_width(
    worksheet: Worksheet, 
    index: pd.Index | pd.MultiIndex, 
    index_width: int | list | None = None, 
    column_width: int | list | None = None) -> None:
    """set index column widths in worksheet"""

    # copy column width
    if index_width is None:
        if isinstance(column_width, int):
            index_width = column_width

    # individual value for all index columns
    if isinstance(index_width, list):

        # check for valid list
        if len(index_width) != index.nlevels:
            raise ValueError("index widths do not match number of levels")

        # set column widths individually
        for col_num, col_width in enumerate(index_width):
            worksheet.set_column(col_num, col_num, col_width)

    # single value for all index columns
    if isinstance(index_width, int):
        worksheet.set_column(0, index.nlevels - 1, index_width)

def _write_index(worksheet: Worksheet, 
    index: pd.Index | pd.MultiIndex,
    row_offset: int,
    index_width: int | list | None = None, 
    column_width: int | list | None = None,
    cell_format: Format | None = None) -> None:
    """write index to worksheet"""

    # set index widths
    _set_index_width(worksheet, index, index_width, column_width)

    # write index names
    if _has_names(index):
        for nr, level in enumerate(index.names):
            worksheet.write(row_offset - 1, nr, level, cell_format)

    # write index values
    if isinstance(index, pd.MultiIndex):

        # write index values for multiindex
        for row_num, row_data in enumerate(index.values):
            for col_num, cell_data in enumerate(row_data):
                worksheet.write(row_num + row_offset, col_num, cell_data)

    else:

        # write index values for regular index
        for row_num, cell_data in enumerate(index.values):
            worksheet.write(row_num + row_offset, 0, cell_data)  

def add_frame(name: str, frame: pd.DataFrame, 
        workbook: Workbook, index: bool = True, 
        column_width: int | list | None = None, 
        index_width: int | list | None = None) -> Worksheet:
    """create worksheet from frame"""

    # add formats
    f1 = workbook.add_format({"bold": True})

    # add worksheet and nan handler
    worksheet = workbook.add_worksheet(str(name))
    worksheet.add_write_handler(float, _handle_nans)

    # set offset
    skiprows = frame.columns.nlevels
    skipcolumns = frame.index.nlevels if index else 0
   
    # write column values
    if isinstance(frame.columns, pd.MultiIndex):

        # modify offset when index names are specified
        if _has_names(frame.index) & (index is True):
            skiprows += 1

        # write column names
        if index is True:
            for nr, level in enumerate(frame.columns.names):
                worksheet.write(nr, skipcolumns - 1, level, f1)

        # write colmns values for multiindex
        for col_num, col_data in enumerate(frame.columns.values):
            for row_num, cell_data in enumerate(col_data):
                worksheet.write(row_num, col_num + skipcolumns, cell_data, f1)

    else:

        # write column values for regular index        
        for col_num, cell_data in enumerate(frame.columns.values):
            worksheet.write(0, col_num + skipcolumns, cell_data, f1)

    # freeze panes with rows and columns
    worksheet.freeze_panes(skiprows, skipcolumns)

   # set column widths
    _set_column_width(worksheet=worksheet, columns=frame.columns, 
        offset=skipcolumns, column_width=column_width)
 
    # write cell values in numeric format    
    for row_num, row_data in enumerate(frame.values):
        for col_num, cell_data in enumerate(row_data):
            worksheet.write(row_num + skiprows, 
                    col_num + skipcolumns, cell_data)

    # write index
    if index is True:
        _write_index(worksheet=worksheet, 
            index=frame.index, row_offset=skiprows, index_width=index_width, 
            column_width=column_width, cell_format=f1)

    return worksheet

def add_series(name: str, series: pd.Series,
    workbook: Workbook, index: bool = True, 
    column_width: int  | None = None, 
    index_width: int | list | None = None) -> Worksheet:

    # add formats
    f1 = workbook.add_format({"bold": True})

    # add worksheet and nan handler
    worksheet = workbook.add_worksheet(str(name))
    worksheet.add_write_handler(float, _handle_nans)

    # set offset and freeze panes
    skipcolumns = series.index.nlevels if index else 0
    worksheet.freeze_panes(1, skipcolumns)

    # handle iterable header
    header = series.name
    if isinstance(header, Iterable) & (not isinstance(header, str)):
        header = '_'.join(map(str, header))

    # write header and set column width
    worksheet.write(0, skipcolumns, header, f1)
    worksheet.set_column(skipcolumns, skipcolumns, column_width)

    # write cell values
    for row_num, cell_data in enumerate(series.values):
        worksheet.write(row_num + 1, skipcolumns, cell_data)

    # include index
    if index is True:
        _write_index(worksheet=worksheet, 
            index=series.index, row_offset=1, index_width=index_width, 
            column_width=column_width, cell_format=f1)

    return worksheet
  