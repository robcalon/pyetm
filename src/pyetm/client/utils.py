"""utility methods"""
from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from pyetm.utils import categorise_curves, regionalise_curves, regionalise_node
from pyetm.types import Carrier

from .session import SessionMethods


class UtilMethods(SessionMethods):
    """utility methods"""

    def categorise_curves(
        self,
        carrier: Carrier,
        mapping: pd.Series[str] | pd.DataFrame,
        columns: list[str] | None = None,
        include_keys: bool = False,
        invert_sign: bool = False,
    ) -> pd.DataFrame:
        """Categorise the hourly curves for a specific carrier with a
        specific mapping.

        Assigns a negative sign to demand to ensure that demand
        and supply keys with the same key mapping can be aggregated.
        This behaviour can be modified with the invert_sign argument.

        Parameters
        ----------
        carrier : str
            The carrier-name of the carrier on which
            the categorization is applied.
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
            specified carrier.
        """

        # make client attribute from carrier
        attribute = f"get_hourly_{carrier}_curves"

        # raise error
        if not hasattr(self, attribute):
            raise NotImplementedError(f'"{attribute}" not implemented')

        # fetch curves
        curves = getattr(self, attribute)()

        # use categorization function
        curves = categorise_curves(
            curves=curves,
            mapping=mapping,
            columns=columns,
            include_keys=include_keys,
            invert_sign=invert_sign,
        )

        return curves

    def regionalise_curves(
        self,
        carrier: Carrier,
        reg: pd.DataFrame,
        node: str | list[str] | None = None,
        sector: str | list[str] | None = None,
        hours: int | list[int] | None = None,
    ) -> pd.DataFrame:
        """Return the residual power curves per node
        based on a regionalisation table.

        Parameters
        ----------
        carrier : str
            The carrier-name for which the
            regionalization is applied.
        reg : DataFrame or str
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
            Residual power profiles."""

        # fetch relevant curves
        if isinstance(carrier, str):
            # make client attribute from carrier
            attribute = f"hourly_{carrier}_curves"

            # fetch curves or raise error
            if hasattr(self, attribute):
                carrier = getattr(self, attribute)

            else:
                # attribute not implemented
                raise NotImplementedError(f'"{attribute}" not implemented')

        if not isinstance(carrier, pd.DataFrame):
            raise TypeError("carrier must be of type string or DataFrame")

        # use regionalisation function
        return regionalise_curves(carrier, reg, node=node, sector=sector, hours=hours)

    def regionalise_node(
        self,
        carrier: Carrier,
        reg: pd.DataFrame,
        node: str,
        sector: str | list[str] | None = None,
        hours: int | list[int] | None = None,
    ) -> pd.DataFrame:
        """Return the sector profiles for a node specified in the
        regionalisation table. The kwargs are passed to pandas.read_csv
        when the regionalisation argument is a passed as a filestring.

        Parameters
        ----------
        carrier : str
            The carrier-name for which the
            regionalization is applied.
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
            Sector profile per specified node."""

        # fetch relevant curves
        if isinstance(carrier, str):
            # make client attribute from carrier
            attribute = f"hourly_{carrier}_curves"

            # fetch curves or raise error
            if hasattr(self, attribute):
                carrier = getattr(self, attribute)

            else:
                # attribute not implemented
                raise NotImplementedError(f'"{attribute}" not implemented')

        if not isinstance(carrier, pd.DataFrame):
            raise TypeError("carrier must be of type string or DataFrame")

        # use regionalisation function
        return regionalise_node(carrier, reg, node, sector=sector, hours=hours)

    def create_hourly_curve_mapping_template(
        self, carriers: str | Iterable[str] | None = None
    ):
        """make output mapping template"""

        # add string to list
        if isinstance(carriers, str):
            carriers = [carriers]

        # carrier for which columns are fetched
        if carriers is None:
            carriers = ["electricity", "heat", "hydrogen", "methane"]

        if not isinstance(carriers, list):
            raise TypeError("carriers must be of type list")

        # regex mapping for product group
        productmap = {
            "^.*[.]output [(]MW[)]$": "supply",
            "^.*[.]input [(]MW[)]$": "demand",
            "deficit": "supply",
        }

        def get_params(carrier):
            """helper for list comprehension"""

            # get curve columns
            curve: pd.DataFrame = getattr(self, f"hourly_{carrier}_curves")

            return pd.Series(data=carrier, index=curve.columns, dtype="str")

        # make output mapping
        cols = [get_params(carrier) for carrier in carriers]
        mapping = pd.concat(cols).to_frame(name="carrier")

        # add product columns
        mapping["product"] = mapping.index.copy()
        mapping["product"] = mapping["product"].replace(productmap, regex=True)

        # set index name
        mapping.index.name = "ETM_key"

        return mapping
