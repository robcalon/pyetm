import logging
import pandas

from pyETM.utils import regionalize_curves

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
