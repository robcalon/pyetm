from __future__ import annotations

import pandas as pd

from typing import Literal
from pyETM.utils import categorise_curves

Carrier = Literal['electricity', 'heat', 'hydrogen', 'methane']


class Categorisation:
    
    def categorise_curves(self, carrier: Carrier, 
        mapping: pd.DataFrame | str, columns: list[str] | None = None, 
        include_keys: bool = False, invert_sign: bool = False, 
        **kwargs) -> pd.DataFrame:
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
            include_keys=include_keys, invert_sign=invert_sign, **kwargs)
    
        return curves
