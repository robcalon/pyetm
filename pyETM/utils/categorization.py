import pandas

class Categorization:
    
    def categorize_curves(self, carrier, mapping, columns=None, 
                          include_index=False, *args, **kwargs):
        """Categorize the hourly curves for a specific carrier with a 
        specific mapping.
        
        Parameters
        ----------
        carrier : str
            The carrier for which the hourly curves are
            categorized.
        mapping : DataFrame or str
            DataFrame with mapping of ETM_IDs. The DataFrame must 
            contain a ETM_ID and ETM_CARRIER column. Alternatively 
            a string to a csv-file can be passed.
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
                        
        # make client attribute from carrier
        attribute = f'hourly_{carrier}_curves'
        
        # fetch curves or raise error
        if hasattr(self, attribute):
            curves = getattr(self, attribute).copy(deep=True)
        
        else:
            # attribute not implemented
            raise NotImplementedError(f'"{attribute}" not implemented')
        
        # load categorization
        if isinstance(mapping, str):
            mapping = pandas.read_csv(mapping, *args, **kwargs)
            
        # check curves mapping
        curves, mapping = self._check_categorization(curves, mapping, carrier)

        # subset columns
        if columns is not None:
            
            # check columns argument
            if isinstance(columns, str):
                columns = [columns]
                
            # subset categorization
            mapping = mapping[columns]
        
        # include index in mapping
        if include_index is True:
        
            # append index as column to mapping
            keys = mapping.index.to_series(name='ETM_ID')
            mapping = pandas.concat([mapping, keys], axis=1)
                
        # make multiindex and midx mapper
        midx = pandas.MultiIndex.from_frame(mapping)
        mapping = dict(zip(mapping.index, midx))
        
        # apply mapping to curves
        curves.columns = curves.columns.map(mapping)
        curves.columns.names = midx.names
        
        # aggregate over levels
        levels = curves.columns.names
        curves = curves.groupby(level=levels, axis=1).sum()

        return curves

    def _check_categorization(self, curves, mapping, carrier):
        """check if all required mappings are present and used"""
        
        # ensure correct midx
        columns = ['ETM_ID', 'ETM_CARRIER']
        if mapping.index.names != columns:
            
            # attempt to construct required index
            drop = isinstance(df.index, pandas.RangeIndex)            
            mapping = mapping.reset_index(drop=drop).set_index(columns)
            
        # subset carrier from mapping
        mapping = mapping.xs(carrier, level='ETM_CARRIER')
        
        # check if passed curves contains columns not specified in cat 
        for item in curves.columns[~curves.columns.isin(mapping.index)]:
            raise KeyError(f'"{item}" is not present in the ' + 
                           f'curve categorization')

        # check if cat specifies keys not in passed curves
        for item in mapping.index[~mapping.index.isin(curves.columns)]:
            raise KeyError(f'"{item}" is not present in the ' +
                           f'returned ETM curves')

        return curves, mapping