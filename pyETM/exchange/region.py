import numpy as np
import pandas as pd

from typing import Optional

from pyETM import Client
from pyETM.logger import get_modulelogger
from pyETM.utils.lookup import lookup_coordinates

logger = get_modulelogger(__name__)


class Region:
    """wrapper around pyETM clients. Is unaware of mappings for
    interconnector keys and only uses ETM keys."""

    @property
    def name(self) -> str:
        """region name"""
        return self.__name

    @property
    def scenario_id(self) -> str:
        """scenario id for region"""
        return self.client.scenario_id

    @property
    def client(self) -> Client:
        """client that connects to ETM"""
        return self.__client

    @property
    def interconnector_names(self) -> pd.Series:
        """names of interconnectors"""
        return self.__interconnector_names

    @property
    def utilization(self) -> pd.DataFrame:

        # create pattern for relevant keys
        p1 = 'interconnector_\d{1,2}_export_availability'
        p2 = 'interconnector_\d{1,2}_import_availability'
        pattern = '%s|%s' %(p1, p2)

        # get attached keys
        keys = self.client.get_custom_curve_keys(False)

        # subset relevant keys
        keys = keys[keys.str.match(pattern)]
        availability = self.custom_curves[keys]

        # base pattern
        base = 'interconnector_\d{1,2}'
        cols = availability.columns

        # subset relevant keys
        suffix = '_import_availability'
        imprt = availability[cols[cols.str.match(base + suffix)]]
        imprt.columns = imprt.columns.str.rstrip(suffix)

        # subset relevant keys
        suffix = '_export_availability'
        exprt = availability[cols[cols.str.match(base + suffix)]]
        exprt.columns = exprt.columns.str.rstrip(suffix)
        
        # get utilization in sparse format
        utilization = imprt.sub(exprt)

        return utilization

    @property
    def etm_utilization(self) -> pd.DataFrame:

        # reference user values
        uvalues = self.client.user_values

        # match all interconnectors
        pattern = "electricity_(interconnector_\d{1,2})_capacity"
        uvalues = uvalues[uvalues.index.str.match(pattern)]

        # subset all active interconnectors
        uvalues = uvalues[uvalues > 0]
        uvalues.index = uvalues.index.str.extract(pattern, expand=False)

        # convert dtype
        uvalues = uvalues.astype('float64')

        # reference hourly ecurves
        curves = self.hourly_electricity_curves

        # helper function
        def process_conn(conn):
            """helper to fetch residual profile"""

            # specify keys
            imprt = "energy_%s_imported_electricity.output (MW)" %conn
            exprt = "energy_%s_exported_electricity.input (MW)" %conn

            # get residual curve
            curve = curves[imprt] - curves[exprt]

            return pd.Series(curve, name=conn, dtype='float64')
        
        # get interconnector utilization profile for ETM curves
        curves = [process_conn(conn) for conn in uvalues.index]
        
        return pd.concat(curves, axis=1) / uvalues

    @utilization.setter
    def utilization(self, utilization: pd.DataFrame) -> None:
        """utilization setter"""

        # import availability
        imprt = utilization.where(utilization > 0, 0)
        imprt.columns += '_import_availability'

        # export availability
        exprt = utilization.where(utilization < 0, 0).abs()
        exprt.columns += '_export_availability'

        # merge and sort availability
        availability = pd.concat([imprt, exprt], axis=1)
        
        # specify curve names
        names = self.interconnector_names
        names = pd.concat([names, names]).to_list()

        # set interconnector availability
        self.client.upload_custom_curves(availability, names=names)

        logger.debug("'%s': uploaded availability curves", self)

    @property
    def exchange_prices(self) -> pd.DataFrame:
        """interconnector price curves"""

        # create pattern for relevant keys
        pattern = 'interconnector_\d{1,2}_price'

        # get attached keys and subset prices
        keys = self.client.get_custom_curve_keys(False)
        keys = keys[keys.str.match(pattern)]

        # get and format price curves
        prices = self.custom_curves[keys]
        prices.columns = prices.columns.str.rstrip('_price')

        return prices

    @property
    def custom_curves(self) -> pd.DataFrame:
        """attached custom curves"""

        # check for cached frames
        cache = self.client.get_custom_curves.cache_info()
        ccurves = self.client.custom_curves

        if cache.hits == 0:
            logger.debug("'%s': downloaded attached ccurves", self)

        return ccurves

    @exchange_prices.setter
    def exchange_prices(self, prices) -> None:
        """interconnector price curves"""
        
        # make key and upload curves
        prices.columns += '_price'
        names = self.interconnector_names.to_list()

        # set price curves
        self.client.upload_custom_curves(prices, names=names)

        logger.debug("'%s': uploaded exchange prices", self)

    @property
    def exchange_capacity(self) -> pd.Series:
        """set capacity for each interconnector"""

        # get pattern and uvalues from client
        pattern = 'electricity_interconnector_\d{1,2}_capacity'
        uvalues = self.client.scenario_parameters

        return uvalues[uvalues.index.str.match(pattern)]

    @property
    def hourly_electricity_curves(self) -> pd.DataFrame:
        """hourly electricity prices curves"""

        # check for cached frames
        cache = self.client.get_hourly_electricity_curves.cache_info()
        curves = self.client.hourly_electricity_curves

        if cache.hits == 0:
            logger.debug("'%s': downloaded electricity curves", self)

        return curves

    @property
    def bidladder(self) -> pd.DataFrame:
        """cache bidladder over all iterations"""
        return self.__bidladder

    @property
    def electricity_price(self) -> pd.Series:
        """hourly electricity price curve"""

        # check for cached frames
        cache = self.client.get_hourly_electricity_price_curve.cache_info()
        prices = self.client.hourly_electricity_price_curve

        if cache.hits == 0:
            logger.debug("'%s': downloaded electricity prices", self)

        return prices

    @property
    def price_setting_unit(self) -> pd.Series:
        """name of price setting unit"""
        
        # get dispatchable utilization
        util = self.dispatchable_utilization.copy()
        
        # invalidate zeros and reverse order
        util = util.replace(0.00, np.nan)
        util = util[util.columns[::-1]]

        # get price setting unit
        unit = util.idxmin(axis=1)
        unit = unit.str.rstrip('.output (MW)')

        return pd.Series(unit, name='unit', dtype='str')

    @property
    def price_setting_capacity(self) -> pd.Series:
        """used capacity of price setting unit"""

        # get units and dispatch
        units = self.price_setting_unit.copy()
        ecurves = self.hourly_electricity_curves

        # get dispatched volume
        units = units + '.output (MW)'
        utilized = lookup_coordinates(units, ecurves).round(2)

        return pd.Series(utilized, name='capacity', dtype='float64')

    @property
    def price_setting_utilization(self) -> pd.Series:
        """utilization of price setting unit"""

        # get units and utilization
        units = self.price_setting_unit.copy()
        utilization = self.dispatchable_utilization

        # lookup utilization
        util = lookup_coordinates(units, utilization)

        return pd.Series(util, name='utilization', dtype='float64')

    @property
    def next_dispatchable_unit(self) -> pd.Series:
        """hourly unit name for next dispatchable unit"""
        
        # get units and match hourly curve format
        ladder = self.bidladder.copy()
        ladder.index = ladder.index + '.output (MW)'

        # get electricity prices and subset relevant curves
        ecurves = self.hourly_electricity_curves
        ecurves = ecurves[ladder.index]

        # find standby unit based on utilization
        # round to prevent merit/python rounding errors
        util = ecurves.div(ladder.capacity).round(2)
        default = util.columns[-1]

        def return_position(utilization):
            """helper to get index of dispatchable"""
            return next((idx for idx, value in utilization.items()
                if value < 1), default)

        # get index of next dispatchable unit
        unit = util.apply(lambda row: return_position(row), axis=1)
        unit = unit.str.rstrip('.output (MW)')

        return pd.Series(unit, name='unit', dtype='str')
        
    @property
    def next_dispatchable_price(self) -> pd.Series:
        """hourly price curve for next dispatchable unit"""
        
        # evaluate prices
        units = self.next_dispatchable_unit
        prices = units.map(self.bidladder.marginal_costs)

        return pd.Series(prices, name='price', dtype='float64')

    @property
    def next_dispatchable_capacity(self) -> pd.Series:
        """hourly surplus capacity of next dispatchable unit"""

        # get bidladder
        ladder = self.bidladder.copy()
        ladder.index = ladder.index + '.output (MW)'

        # get hourly electricity curves
        ecurves = self.hourly_electricity_curves
        ecurves = ecurves[ladder.index]

        # get units
        units = self.next_dispatchable_unit.copy()
        units = units + '.output (MW)'

        # get capacity and dispatch
        capacity = units.map(ladder.capacity).round(2)
        utilized = lookup_coordinates(units, ecurves).round(2)

        # evaluate capacity
        capacity = capacity - utilized

        return pd.Series(capacity, name='capacity', dtype='float64')

    @property
    def dispatchable_utilization(self) -> pd.DataFrame:

        # get bidladder
        ladder = self.bidladder.copy()
        ladder.index = ladder.index + '.output (MW)'

        # get hourly electricity curves
        ecurves = self.hourly_electricity_curves
        ecurves = ecurves[ladder.index]

        # evaluate capacity and strip suffix
        ecurves = ecurves / ladder.capacity
        ecurves.columns = ecurves.columns.str.rstrip('.output (MW)')

        return ecurves.round(2)

    # @property
    # def surplus_bidlevel(self) -> pd.Series:
    #     """reached bidlevel of priceladder at each hour of the year. 
    #     The reached bidlevel is based on the hourly electricity prices."""

    #     # get dispatchables and match hourly curve format
    #     ladder = self.bidladder.copy()
    #     ladder.index = ladder.index + '.output (MW)'

    #     # get hourly electricity price curve
    #     prices = self.electricity_price

    #     # reference vars for helper function
    #     mcosts = ladder.marginal_costs
    #     default = mcosts.index[-1]

    #     def return_position(price):
    #         """helper to get index of dispatchable"""
    #         return next((idx for idx, value in mcosts.items()
    #             if price <= value), default)

    #     # get index of active bidladder at eprice
    #     bidlevel = prices.apply(return_position)
    #     bidlevel = bidlevel.str.rstrip('.output (MW)')

    #     return pd.Series(bidlevel, name='bidlevel', dtype='str')

    # @property
    # def surplus_capacity(self) -> pd.Series:
    #     """"capacity surplus at each hour. Based on the reached
    #     bidlevel for the electricity price in each hour."""

    #     # get bidladder
    #     ladder = self.bidladder.copy()
    #     ladder.index = ladder.index + '.output (MW)'

    #     # get hourly electricity curves
    #     curves = self.hourly_electricity_curves

    #     # determine dispatchable utilization
    #     # correction for rounding errors in merit
    #     curves = curves[ladder.index]
    #     util = curves.sum(axis=1).round(2)

    #     # get bidlevel
    #     bidlevel = self.surplus_bidlevel
    #     bidlevel = bidlevel + '.output (MW)'

    #     # determine capacity 
    #     # correction for rounding erros
    #     capacity = ladder.capacity.cumsum()
    #     capacity = bidlevel.map(capacity).round(2)

    #     return capacity - util

    def __init__(self, name: str, scenario_id = str, 
            reset: bool = True, capacities: Optional[pd.Series] = None, 
            interconnector_names: Optional[pd.Series] = None, **kwargs) -> None:
        """kwargs used for client, be aware that the proxies
        argument changes to proxy when enabling a queue."""

        # set properties
        self.__name = name
        self.__client = Client(scenario_id, **kwargs)
        self.__interconnector_names = interconnector_names

        # reset interconnectors
        if reset:
            self.__reset_interconnectors(capacities)

        # cache bidladder in region
        self.__bidladder = self.client.get_dispatchables_bidladder()
        logger.debug("'%s': downloaded bidladder", self)

    def __repr__(self) -> str:
        """reproduction string"""
        return "Region(%s, %s)" %(self.name, self.scenario_id)

    def __str__(self) -> str:
        """string name"""
        return self.name.upper()

    def cache_properties(self) -> None:
        """ensure all variables are cached"""

        # call relevant variables
        self.utilization
        self.electricity_price
        self.hourly_electricity_curves

    def __reset_interconnectors(self, capacities: pd.Series) -> None:
        """sets capacities of specified interconnectors and 
        disables all other interconnectors. All availability 
        and price related custom curves are removed from all 
        interconnectors.
        
        capacities : pd.Series
            interconnector name ('interconnector_x') in index
            and capacity of interconnector as value."""

        # drop capacities with non-positive capacities
        capacities = capacities[capacities > 0]

        # get uvalues for client
        uvalues = self.client.scenario_parameters

        # subset interconnectors
        pattern = 'electricity_interconnector_\d{1,2}_capacity'
        conns = uvalues[uvalues.index.str.match(pattern)]

        # set interconnection 
        # capacity to zero
        conns.loc[:] = 0
        conns.name = 'ETM_key'

        # check if new capacities need to be placed
        if capacities is not None:

            # update capacities
            conns.update(capacities)
            self.client.user_values = conns

            logger.debug("'%s': uploaded capacity settings", self)

            # create pattern for relevant keys
            p1 = 'interconnector_\d{1,2}_export_availability'
            p2 = 'interconnector_\d{1,2}_import_availability'
            pattern = '%s|%s' %(p1, p2)

            # get all keys and subset availability keys
            keys = self.client.get_custom_curve_keys(True)
            keys = keys[keys.str.match(pattern)]

            # subset interconnectors that are set
            conns = range(1, len(capacities) + 1)
            pattern = [f"interconnector_{nr}_" for nr in conns]

            # check pattern
            pattern = "|".join(pattern)
            columns = keys[keys.str.contains(pattern, regex=True)]

            # get interconnectors
            pattern = '(interconnector_\d{1,2})'
            ccolumns = columns.str.extract(pattern, expand=False).unique()
            
            # replace existing keys
            utilization = pd.DataFrame(0, index=range(8760), columns=ccolumns)
            self.utilization = utilization

            logger.debug("'%s': uploaded initial ccurves", self)

        else:
            
            logger.debug("'%s': all capacities set to zero", self)

        # get custom curve keys without unattached
        keys = self.client.get_custom_curve_keys(False)

        # check for keys that need updates
        if capacities is not None:

            # subset keys that need to be unattached
            keys = keys[~keys.isin(columns)]

        # unattach keys
        if not keys.empty:

            # delete custom curves
            self.client.delete_custom_curves(keys=keys)
            logger.debug("'%s': deleted superfluous ccurves", self)
