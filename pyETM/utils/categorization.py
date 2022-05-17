import pandas

def categorize_curves(curves, mapping, columns=None,
                      include_index=False, *args, **kwargs):
    """Categorize the hourly curves for a specific dataframe 
    with a specific mapping.
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
    
    # copy curves
    curves = curves.copy()

    # load categorization
    if isinstance(mapping, str):
        mapping = pandas.read_csv(mapping, *args, **kwargs)
        
    # check if passed curves contains columns not specified in cat 
    for item in curves.columns[~curves.columns.isin(mapping.index)]:
        raise KeyError(f'"{item}" is not present in the ' + 
                       f'curve categorization')

    # check if cat specifies keys not in passed curves
    for item in mapping.index[~mapping.index.isin(curves.columns)]:
        raise KeyError(f'"{item}" is not present in the ' +
                       f'returned ETM curves')

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
        keys = mapping.index.to_series(name='ETM_key')
        mapping = pandas.concat([mapping, keys], axis=1)

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
        midx = pandas.MultiIndex.from_frame(mapping)
        mapping = dict(zip(mapping.index, midx))

        # apply mapping to curves
        curves.columns = curves.columns.map(mapping)
        curves.columns.names = midx.names

        # aggregate over levels
        levels = curves.columns.names
        curves = curves.groupby(level=levels, axis=1).sum()

    return curves