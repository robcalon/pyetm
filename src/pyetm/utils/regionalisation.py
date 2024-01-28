"""regionalisation methods"""
from __future__ import annotations

import logging
import pandas as pd

from pyetm.exceptions import BalanceError
from pyetm.types import ErrorHandling
from pyetm.utils.categorisation import assigin_sign_convention
from pyetm.utils.general import iterable_to_str, mapped_floats_to_str

logger = logging.getLogger(__name__)


def is_hourly_balanced_curves(
    curves: pd.DataFrame,
    precision: int = 1,
    errors: ErrorHandling = "warn"
) -> bool:
    """validate if deficits in curves"""

    # check if mapping is already applied
    pattern = "(^.*[.]output [(]MW[)]$|^.*[.]input [(]MW[)]$)"
    if any(curves.columns.get_level_values(level=-1).str.match(pattern)):
        curves = assigin_sign_convention(curves)

    # validate balance of curves
    balance = curves.sum(axis=1)
    if any(balance.round(precision) != 0):

        # report inbalance
        message = "Deficits in hourly carrier curves"

        if errors == "ignore":
            logger.debug(message)

        if errors == "warn":
            logger.warning(message)

        if errors == "raise":
            raise BalanceError(message)

        return False
    return True

def is_valid_regionalisation(
    curves: pd.DataFrame,
    reg: pd.DataFrame,
    prec: int = 3,
    errors: ErrorHandling = "warn",
) -> bool:
    """helper function to validate regionalisation table"""

    # check if passed curves specifies keys not specified in reg
    missing_reg = curves.columns[~curves.columns.isin(reg.columns)]
    if not missing_reg.empty:
        raise KeyError(
            f"Missing key(s) in regionalisation: {iterable_to_str(missing_reg)}"
        )

    # check is reg specifies keys not in passed curves
    superfluos_reg = reg.columns[~reg.columns.isin(curves.columns)]
    if not superfluos_reg.empty:
        if errors == "warn":
            for key in superfluos_reg:
                logger.warning("Unused key in regionalisation: %s", key)

        if errors == "raise":
            error = iterable_to_str(superfluos_reg)
            raise KeyError(f"Unused key(s) in regionalisation: {error}")

    # check if regionalizations add up to 1.000
    sums = reg.sum(axis=0).round(prec)
    checksum_errors = sums[sums != 1]
    if not checksum_errors.empty:

        if errors == "ignore":
            for key, value in checksum_errors.items():
                error = f"{key}={value:.{prec}f}"
                logger.debug("Regionalisation key does not sum to 1: %s", error)

        if errors == "warn":
            for key, value in checksum_errors.items():
                error = f"{key}={value:.{prec}f}"
                logger.warning("Regionalisation key does not sum to 1: %s", error)

        if errors == "raise":
            error = mapped_floats_to_str((dict(checksum_errors)), prec=prec)
            raise ValueError(f"Regionalisation key(s) do not sum to 1: {error}")

        return False
    return True

def regionalise_curves(
    curves: pd.DataFrame,
    reg: pd.DataFrame,
    node: str | list[str] | None = None,
    sector: str | list[str] | None = None,
    hours: int | list[int] | None = None,
) -> pd.DataFrame:
    """
    Return the residual power of the curves based on a regionalisation table.

    Parameters
    ----------
    curves : DataFrame
        Categorized ETM curves.
    reg : DataFrame
        Regionalization table with nodes in index and
        sectors in columns.
    node : key or list of keys, default None
        Specific node in regionalisation for which
        the dot product is evaluated, defaults to all nodes.
    sector : key or list of keys, default None
        Specific sector in regionalisation for which
        the dot product is evaluated, defaults to all sectors.
    hours : key or list of keys, default None
        Specific hours for which the dot product
        is evaluated, defaults to all hours.

    Return
    ------
    curves : DataFrame
        Residual power curves per regionalisation node.
    """

    # validate regionalisation
    is_hourly_balanced_curves(curves, errors="raise")
    is_valid_regionalisation(curves, reg)

    # handle node subsetting
    if node is not None:
        # warn for subsettign multiple items
        if isinstance(node, list):
            logger.warning("returning dot product for subset of multiple nodes")

        # handle string
        if isinstance(node, str):
            node = [node]

        # subset node
        reg = reg.loc[node, :]

    # handle sector subsetting
    if sector is not None:
        # warn for subsetting multiple items
        if isinstance(sector, list):
            logger.warning("returning dot product for subset of multiple sectors")

        # handle string
        if isinstance(sector, str):
            sector = [sector]

        # subset sector
        curves, reg = curves.loc[:, sector], reg.loc[:, sector]

    # subset hours
    if hours is not None:
        # handle single hour
        if isinstance(hours, int):
            hours = [hours]

        # subset hours
        curves = curves.iloc[hours, :]

    return curves.dot(reg.T)


def regionalise_node(
    curves: pd.DataFrame,
    reg: pd.DataFrame,
    node,
    sector = None,
    hours = None,
) -> pd.DataFrame:
    """
    Return the sector profiles for a node specified in the regionalisation table

    Parameters
    ----------
    curves : DataFrame
        Categorized ETM curves.
    reg : DataFrame or str
        Regionalization table with nodes in index and
        sectors in columns.
    node : key
        Specific node in regionalisation for which
        the profiles are returned.
    sector : key or list of keys, default None
        Specific sector in regionalisation for which
        the profile is evaluated, defaults to all sectors.
    hours : key or list of keys, default None
        Specific hours for which the profiles
        are evaluated, defaults to all hours.

    Return
    ------
    curves : DataFrame
        Sector profile per specified node.
    """

    # validate regionalisation
    is_hourly_balanced_curves(curves)
    is_valid_regionalisation(curves, reg)

    # subset reg for node
    nreg = reg.loc[node, :]

    # handle sector subsetting
    if sector is not None:
        if isinstance(sector, str):
            sector = [sector]

        # subset sector
        curves, nreg = curves.loc[:, sector], nreg.loc[sector]

    # subset hours
    if hours is not None:
        # handle single hour
        if isinstance(hours, int):
            hours = [hours]

        # subset hours
        curves = curves.iloc[hours, :]

    return curves.mul(nreg)
