"""categorisation method"""
from __future__ import annotations
from typing import Iterable
import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.types import ErrorHandling
from pyetm.utils.general import iterable_to_str

logger = get_modulelogger(__name__)


def assigin_sign_convention(
    curves: pd.DataFrame,
    invert_sign: bool = False,
    pattern: str | None = None
) -> pd.DataFrame:
    """
    This function applies a negative sign to the default demand
    keys in the passed hourly carrier curves. The sign convention can be
    optionally be inverted.

    Parameters
    ----------
    curves : DataFrame
        Hourly carrier curves.
    invert_sign : bool, default False
        Inverts assigned sign convention by assigning
        a negative sign to supply instead of demand.
    pattern: str, default None
        Regex pattern with which the profile keys are
        identified that are modified.

    Return
    ------
    curves : DataFrame
        Hourly carrier curves with sign convention.
    """

    # ensure etm convention
    curves = curves.abs()

    # default etm patterns
    if pattern is None:
        pattern = "^.*[.]input [(]MW[)]$"

    # validate pattern is present
    if not any(curves.columns.get_level_values(level=-1).str.match(pattern)):
        raise KeyError(f"Could not find pattern in hourly curves: '{pattern}'")

    # subset relevant columns
    cols = curves.columns.get_level_values(
        level=-1).str.contains(pattern, regex=True)

    # invert selected columns
    if invert_sign is True:
        cols = ~cols

    # apply sign convention
    curves.loc[:, cols] = -curves.loc[:, cols]

    return curves.replace(-0, 0)


def validate_categorisation(
    curves: pd.DataFrame,
    mapping: pd.Series[str] | pd.DataFrame,
    errors: ErrorHandling = "warn",
) -> None:
    """validate categorisation"""

    # check if passed curves contains columns not specified in cat
    missing_curves = curves.columns[~curves.columns.isin(mapping.index)]
    if not missing_curves.empty:
        missing = "', '".join(map(str, missing_curves))
        raise KeyError(f"Missing key(s) in mapping: '{missing}'")

    # check if cat specifies keys not in passed curves
    superfluous_curves = mapping.index[~mapping.index.isin(curves.columns)]
    if not superfluous_curves.empty:
        if errors == "warn":
            for key in superfluous_curves:
                logger.warning("Unused key in mapping: %s", key)

        if errors == "raise":
            error = iterable_to_str(superfluous_curves)
            raise ValueError(f"Unsued key(s) in mapping: {error}")


def categorise_curves(
    curves: pd.DataFrame,
    mapping: pd.Series[str] | pd.DataFrame,
    columns: str | Iterable[str] | None = None,
    include_keys: bool = False,
    invert_sign: bool = False,
) -> pd.DataFrame:
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
    mapping : DataFrame
        DataFrame with mapping of ETM keys in index and mapping
        values in columns.
    columns : list, default None
        List of column names and order that will be included
        in the mapping. Defaults to all columns in mapping.
    include_keys : bool, default False
        Include the original ETM keys in the resulting mapping.
    invert_sign : bool, default False
        Inverts sign convention where demand is denoted with
        a negative sign. Demand will be denoted with a positve
        value and supply with a negative value.

    Return
    ------
    curves : DataFrame
        DataFrame with the categorized curves of the
        specified carrier."""

    # copy curves
    curves = curves.copy()

    if isinstance(mapping, pd.Series):
        columns = mapping.to_frame().columns

    if curves.columns.nlevels != mapping.index.nlevels:
        raise ValueError("Index levels of 'curves' and 'mapping' are not alligned")

    # apply sign convention
    validate_categorisation(curves, mapping)
    curves = assigin_sign_convention(curves, invert_sign=invert_sign)

    # default columns
    if columns is None:
        columns = mapping.columns

    # check columns argument
    if isinstance(columns, str):
        columns = [columns]

    # subset categorization
    mapping = mapping.loc[:, columns]

    # include index in mapping
    if include_keys is True:
        # append index as column to mapping
        keys = mapping.index.to_series(name="ETM_key")
        mapping = pd.concat([mapping, keys], axis=1)

    # include levels in mapping
    if mapping.index.nlevels > 1:
        # transform index levels to frame
        idx = mapping.index.droplevel(level=-1)
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
        curves = curves.T.groupby(by=column).sum().T

    else:
        # make mapper for multiindex
        names = mapping.columns
        mapper = dict(zip(mapping.index, pd.MultiIndex.from_frame(mapping)))

        # apply mapping to curves
        midx = curves.columns.to_series().map(mapper)
        curves.columns = pd.MultiIndex.from_tuples(midx, names=names)

        # aggregate over levels
        curves = curves.T.groupby(level=list(names)).sum().T

    return curves.sort_index(axis=1)
