import pandas

from pyETM.logger import get_modulelogger
from pyETM.utils import regionalise_curves, regionalise_node

# get modulelogger
logger = get_modulelogger(__name__)


class Regionalisation:
    
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

        if not isinstance(carrier, pandas.DataFrame):
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

        if not isinstance(carrier, pandas.DataFrame):
            raise TypeError('carrier must be of type string or DataFrame')

        # use regionalisation function
        return regionalise_node(carrier, reg, node, 
                                sector=sector, hours=hours, **kwargs)
    