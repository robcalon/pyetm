"""categorisation method"""
from __future__ import annotations

import pandas as pd
from pyetm.logger import get_modulelogger

logger = get_modulelogger(__name__)


def categorise_curves(curves: pd.DataFrame,
    mapping: pd.DataFrame | str, columns: list[str] | None =None,
    include_keys: bool = False, invert_sign: bool = False,
    pattern_level: str | int | None = None, **kwargs) -> pd.DataFrame:
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
    pattern_level : str or int, default None
        Column level in which sign convention pattern is located.
        Assumes last level by default.

    **kwargs are passed to pd.read_csv when a filename is
    passed in the mapping argument.

    Return
    ------
    curves : DataFrame
        DataFrame with the categorized curves of the
        specified carrier.
    """
    # copy curves
    curves = curves.copy()

    # load categorization
    if isinstance(mapping, str):
        mapping = pd.read_csv(mapping, **kwargs)

    if isinstance(mapping, pd.Series):
        mapping = mapping.to_frame()
        columns = mapping.columns

    if curves.columns.nlevels != mapping.index.nlevels:
        raise ValueError(
            "Index levels of 'curves' and 'mapping' are not alligned")

    # check if passed curves contains columns not specified in cat
    missing_curves = curves.columns[~curves.columns.isin(mapping.index)]
    if not missing_curves.empty:

        # make message
        missing_curves = "', '".join(map(str, missing_curves))
        message = f"Missing key(s) in mapping: '{missing_curves}'"

        raise KeyError(message)

    # check if cat specifies keys not in passed curves
    superfluous_curves = mapping.index[~mapping.index.isin(curves.columns)]
    if not superfluous_curves.empty:

        # make message
        superfluous_curves = "', '".join(map(str, superfluous_curves))
        message = f"Unused key(s) in mapping: '{superfluous_curves}'"

        logger.warning(message)

    # determine pattern for desired sign convention
    pattern = '[.]output [(]MW[)]' if invert_sign else '[.]input [(]MW[)]'

    # assume pattern in last level
    if pattern_level is None:
        pattern_level = curves.columns.nlevels - 1

    # subset column positions with pattern
    cols = curves.columns.get_level_values(
        level=pattern_level).str.contains(pattern)

    # assign sign convention by pattern
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
        keys = mapping.index.to_series(name='ETM_key')
        mapping = pd.concat([mapping, keys], axis=1)

    # include levels in mapping
    if mapping.index.nlevels > 1:

        # transform index levels to frame
        idx = mapping.index.droplevel(level=pattern_level)
        idx = idx.to_frame(index=False).set_index(mapping.index)

        # join frame with mapping
        mapping = pd.concat([idx, mapping], axis=1)

    if len(mapping.columns) == 1:

        # extract column
        column = columns[0]

        # apply mapping to curves
        curves.columns = curves.columns.map(mapping[column])
        curves.columns.name = column

        # aggregate over mapping
        curves = curves.groupby(by=column, axis=1).sum()

    else:

        # make mapper for multiindex
        names = list(mapping.columns)
        mapping = dict(zip(mapping.index, pd.MultiIndex.from_frame(mapping)))

        # apply mapping to curves
        midx = curves.columns.to_series().map(mapping)
        curves.columns = pd.MultiIndex.from_tuples(midx, names=names)

        # aggregate over levels
        curves = curves.groupby(level=names, axis=1).sum()

    return curves.sort_index(axis=1)
