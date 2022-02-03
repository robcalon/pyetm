import logging
import pandas as pd

logger = logging.getLogger(__name__)

def regionalize_curves(curves, reg, chunksize=500, **kwargs):
    """regionalize the curves based on a regionalization table. The
    (kw)args are passed to pandas.read_csv if the regionalization is 
    passed as a filestring.
    
    Parameters
    ----------
    curves : DataFrame
        Categorized ETM curves.
    reg : DataFrame
        Regionalization table with nodes in index and 
        sectors in columns.
    chunksize : int, default 500
        Number of hours regionalized at once. Regionalizing
        large curvesets increases memory pressure.
     
     Return
     ------
     curves : DataFrame
         Regionalized ETM curves.
     """

    # load regioanlization
    if isinstance(reg, str):
        reg = pd.read_csv(reg, **kwargs)
        
    # check is reg specifies keys not in passed curves
    for item in reg.columns[~reg.columns.isin(curves.columns)]:
        raise ValueError("'%s' is not present in the passed curves" %item)

    # check if passed curves specifies keys not specified in reg
    for item in curves.columns[~curves.columns.isin(reg.columns)]:
        raise ValueError("'%s' not present in the regionalization" %item)

    # check if regionalizations add up to 1.000
    sums = round(reg.sum(axis=0), 3)
    for idx, value in sums[sums != 1].iteritems():
        raise ValueError(f'"{idx}" regionalization sums to ' +
                         f'{value: .3f} instead of 1.000')

    def chunker(df, size): 
        """chucks dataframe in smaller chuncks"""

        chunks = len(df) // size
        if len(df) % size != 0:
            chunks += 1

        for i in range(chunks):
            yield df[i * size: (i + 1) * size]
    
    # regionalize curves chunkwize
    curves = [_regionalize_curves(ck, reg) for ck in chunker(curves, chunksize)]
    
    return pd.concat(curves)
        
        
def _regionalize_curves(curves, reg):
    
    # prepare new index
    levels = [curves.index, reg.index]
    index = pd.MultiIndex.from_product(levels)

    # prepare new dataframe
    columns = curves.columns
    values = np.repeat(curves.values, reg.index.size, axis=0)

    # match index structure of regionalization
    curves = pd.DataFrame(values, index=index, columns=columns)
    
    return reg.mul(curves, level=1)
