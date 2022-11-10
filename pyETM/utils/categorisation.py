from __future__ import annotations

import pandas as pd

from pyETM.logger import get_modulelogger

logger = get_modulelogger(__name__)


def categorise_curves(curves: pd.DataFrame, 
    mapping: pd.DataFrame | str, columns: list[str] | None =None, 
    include_keys: bool = False, invert_sign: bool = False, 
    **kwargs) -> pd.DataFrame:
    """Categorize the hourly curves for a specific dataframe
    with a specific mapping. 
    
    Assigns a negative sign to demand to ensure that demand 
    and supply keys with the same key mapping can be aggregated. 
    This behaviour can be modified with the invert_sign argument. 

    Parameters
    ----------
    curves : DataFrame
        The hourly curves for which the
        categorization is applied.
    mapping : DataFrame or str
        DataFrame with mapping of ETM keys in index and mapping
        values in columns. Alternatively a string to a csv-file
        can be passed.
    columns : list, default None
        List of column names and order that will be included
        in the mapping. Defaults to all columns in mapping.
    include_keys : bool, default False
        Include the original ETM keys in the resulting mapping.
    invert_sign : bool, default False
        Inverts sign convention where demand is denoted with 
        a negative sign. Demand will be denoted with a positve
        value and supply with a negative value.
    
    **kwargs are passed to pd.read_csv when a filename is
    passed in the mapping argument.

    Return
    ------
    curves : DataFrame
        DataFrame with the categorized curves of the
        specified carrier.
    """

    # load categorization
    if isinstance(mapping, str):
        mapping = pd.read_csv(mapping, **kwargs)

    if isinstance(mapping, pd.Series):
        mapping = mapping.to_frame()
        columns = mapping.columns

    # check if passed curves contains columns not specified in cat
    missing_curves = curves.columns[~curves.columns.isin(mapping.index)]
    if not missing_curves.empty:

        # make message
        missing_curves = "', '".join(map(str, missing_curves))
        message = "Missing key(s) in mapping: '%s'" %missing_curves

        raise KeyError(message)

    # check if cat specifies keys not in passed curves
    superfluous_curves = mapping.index[~mapping.index.isin(curves.columns)]
    if not superfluous_curves.empty:

        # make message
        superfluous_curves = "', '".join(map(str, superfluous_curves))
        message = "Unused key(s) in mapping: '%s'" %superfluous_curves
        
        logger.warning(message)

    # copy curves
    curves = curves.copy()

    # determine pattern for desired sign convention
    pattern = '.output (MW)' if invert_sign else '.input (MW)'

    # assign sign convention by pattern
    cols = curves.columns.str.contains(pattern, regex=False)
    curves.loc[:, cols] = -curves.loc[:, cols]

    # subset columns
    if columns is not None:

        # check columns argument
        if isinstance(columns, str):
            columns = [columns]

        # subset categorization
        mapping = mapping[columns]

    # include index in mapping
    if include_keys is True:

        # append index as column to mapping
        keys = mapping.index.to_series(name='KEY')
        mapping = pd.concat([mapping, keys], axis=1)

    if len(mapping.columns) == 1:

        # extract column
        column = columns[0]

        # apply mapping to curves
        curves.columns = curves.columns.map(mapping[column])
        curves.columns.name = column

        # aggregate over mapping
        curves = curves.groupby(by=column, axis=1).sum()

    else:

        # make multiindex and midx mapper
        midx = pd.MultiIndex.from_frame(mapping)
        mapping = dict(zip(mapping.index, midx))

        # apply mapping to curves
        curves.columns = curves.columns.map(mapping)
        curves.columns.names = midx.names

        # aggregate over levels
        levels = curves.columns.names
        curves = curves.groupby(level=levels, axis=1).sum()

    return curves


def diagnose_categorisation(mapping, curves, warn=True):
    """Diagnose categorisation keys

    Parameters
    ----------
    mapping : dict or Series
        ETM keys in index and user keys as values.
    curves : DataFrame
        Uncategorised ETM curves.
    warn : bool, default True
        Raise warning if check failed."""

    # initialize dict
    diagnosis = {}

    # convert dict to series
    if isinstance(mapping, dict):
        mapping = pd.Series(mapping)

    # identify invalid entries
    errors = mapping.index[~mapping.index.isin(curves.columns)]

    # store errors
    if not errors.empty:
        diagnosis["invalid"] = set(errors)

    # identify missing entries
    errors = curves.columns[~curves.columns.isin(mapping.index)]

    # store errors
    if not errors.empty:
        diagnosis["missing"] = set(errors)

    # raise warning
    if bool(diagnosis) & warn:
        logger.warning("regionalisation contains errors")

    return diagnosis
