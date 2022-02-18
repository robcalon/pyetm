import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

class Regionalization:
    
    def regionalize_curves(self, carrier, reg, chunksize=500, **kwargs):
        """Regionalize the curves based on a regionalization table. The
        (kw)args are passed to pandas.read_csv if the regionalization is 
        passed as a filestring.
        
        Parameters
        ----------
        carrier : str or DataFrame
            The carrier-name or hourly curves for which the 
            regionalization is applied.
        reg : DataFrame
            Regionalization table with nodes in index and 
            sectors in columns.
        chunksize : int, default 500
            Number of hours regionalized at once. Regionalizing
            large curvesets increases memory pressure.
            
        *args and **kwargs arguments are passed to pandas.read_csv when
        a filename is passed in the reg argument.
        
        Return
        ------
        curves : DataFrame
            DataFrame with the regioanlized curves.
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
        curves = regionalize_curves(carrier, reg, chunksize, **kwargs)
    
        return curves


def regionalize_curves(curves, reg, chunksize=500, **kwargs):
    """Regionalize the curves based on a regionalization table. The
    (kw)args are passed to pandas.read_csv if the regionalization is 
    passed as a filestring.
    
    Parameters
    ----------
    curves : DataFrame
        Categorized ETM curves.
    reg : DataFrame
        Regionalization table with nodes in index and 
        sectors in columns.
        
    Return
    ------
    curves : DataFrame
        Regionalized ETM curves.
    """

    # load regioanlization
    if isinstance(reg, str):
        reg = pd.read_csv(reg, **kwargs)

    # check if curves is series object
    if isinstance(curves, pd.Series):
        curves = curves.to_frame().T
        
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

    # prepare new index
    levels = [curves.index, reg.index]
    index = pd.MultiIndex.from_product(levels)

    # prepare new dataframe
    columns = curves.columns
    values = np.repeat(curves.values, reg.index.size, axis=0)

    # match index structure of regionalization
    curves = pd.DataFrame(values, index=index, columns=columns)
    
    return reg.mul(curves, level=1)
