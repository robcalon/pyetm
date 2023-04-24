"""utility methods"""
from __future__ import annotations

from typing import Literal
import pandas as pd

from pyetm.utils import categorise_curves, regionalise_curves, regionalise_node
from .session import SessionMethods

Carrier = Literal['electricity', 'heat', 'hydrogen', 'methane']


class UtilMethods(SessionMethods):
    """utility methods"""

    def categorise_curves(self, carrier: Carrier,
        mapping: pd.DataFrame | str, columns: list[str] | None = None,
        include_keys: bool = False, invert_sign: bool = False,
        pattern_level: str | int | None = None, **kwargs) -> pd.DataFrame:
        """Categorise the hourly curves for a specific carrier with a
        specific mapping.

        Assigns a negative sign to demand to ensure that demand
        and supply keys with the same key mapping can be aggregated.
        This behaviour can be modified with the invert_sign argument.

        Parameters
        ----------
        carrier : str or DataFrame
            The carrier-name or hourly curves for which the
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



        **kwargs arguments are passed to pd.read_csv when
        a filename is passed in the mapping argument.

        Return
        ------
        curves : DataFrame
            DataFrame with the categorized curves of the
            specified carrier.
        """

        # fetch relevant curves
        if isinstance(carrier, str):

            # make client attribute from carrier
            attribute = f'hourly_{carrier}_curves'

            # fetch curves or raise error
            if hasattr(self, attribute):
                carrier = getattr(self, attribute)

            else:
                # attribute not implemented
                raise NotImplementedError(f'"{attribute}" not implemented')

        if not isinstance(carrier, pd.DataFrame):
            raise TypeError('carrier must be of type string or DataFrame')

        # use categorization function
        curves = categorise_curves(
            curves=carrier, mapping=mapping, columns=columns,
            include_keys=include_keys, invert_sign=invert_sign,
            pattern_level=pattern_level, **kwargs)

        return curves

    def regionalise_curves(self, carrier, reg, node=None,
                           sector=None, hours=None, **kwargs):
        """Return the residual power of the curves based on a
        regionalisation table. The kwargs are passed to pandas.read_csv
        when the regionalisation argument is a passed as a filestring.

        Parameters
        ----------
        carrier : str or DataFrame
            The carrier-name or hourly curves for which the
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
            attribute = f'hourly_{carrier}_curves'

            # fetch curves or raise error
            if hasattr(self, attribute):
                carrier = getattr(self, attribute)

            else:
                # attribute not implemented
                raise NotImplementedError(f'"{attribute}" not implemented')

        if not isinstance(carrier, pd.DataFrame):
            raise TypeError('carrier must be of type string or DataFrame')

        # use regionalisation function
        return regionalise_curves(carrier, reg, node=node,
                                  sector=sector, hours=hours, **kwargs)

    def regionalise_node(self, carrier, reg, node,
                         sector=None, hours=None, **kwargs):

        """Return the sector profiles for a node specified in the
        regionalisation table. The kwargs are passed to pandas.read_csv
        when the regionalisation argument is a passed as a filestring.

        Parameters
        ----------
        carrier : str or DataFrame
            The carrier-name or hourly curves for which the
            regionalization is applied.
        reg : DataFrame or str
            Regionalization table with nodes in index and
            sectors in columns.
        node : key or list of keys
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
            attribute = f'hourly_{carrier}_curves'

            # fetch curves or raise error
            if hasattr(self, attribute):
                carrier = getattr(self, attribute)

            else:
                # attribute not implemented
                raise NotImplementedError(f'"{attribute}" not implemented')

        if not isinstance(carrier, pd.DataFrame):
            raise TypeError('carrier must be of type string or DataFrame')

        # use regionalisation function
        return regionalise_node(carrier, reg, node,
                                sector=sector, hours=hours, **kwargs)

    def make_output_mapping_template(self, carriers=None):
        """make output mapping template"""

        # add string to list
        if isinstance(carriers, str):
            carriers = [carriers]

        # carrier for which columns are fetched
        if carriers is None:
            carriers = ['electricity', 'heat', 'hydrogen', 'methane']

        if not isinstance(carriers, list):
            raise TypeError('carriers must be of type list')

        # regex mapping for product group
        productmap = {
            '^.*[.]output [(]MW[)]$': 'supply',
            '^.*[.]input [(]MW[)]$': 'demand',
            'deficit': 'supply',
        }

        def get_params(carrier):
            """helper for list comprehension"""

            # get curve columns
            curve = f'hourly_{carrier}_curves'
            idx = getattr(self, curve).columns

            return pd.Series(data=carrier, index=idx, dtype='str')

        # make output mapping
        mapping = [get_params(carrier) for carrier in carriers]
        mapping = pd.concat(mapping).to_frame(name='carrier')

        # add product columns
        mapping['product'] = mapping.index.copy()
        mapping['product'] = mapping['product'].replace(productmap, regex=True)

        # set index name
        mapping.index.name = 'ETM_key'

        return mapping
