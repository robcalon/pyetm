import pandas
from pyETM.utils import categorise_curves


class Categorisation:
    
    def categorise_curves(self, carrier, mapping, columns=None, 
                          include_index=False, *args, **kwargs):
        """Categorise the hourly curves for a specific carrier with a 
        specific mapping.
        
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
        include_index : bool, default False
            Include the original ETM keys in the resulting mapping.
            
        *args and **kwargs arguments are passed to pandas.read_csv when
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
        
        if not isinstance(carrier, pandas.DataFrame):
            raise TypeError('carrier must be of type string or DataFrame')
        
        # use categorization function
        curves = categorise_curves(carrier, mapping, columns, 
                                   include_index, *args, **kwargs)
    
        return curves
