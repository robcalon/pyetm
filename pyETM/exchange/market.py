
from __future__ import annotations

import shutil

import numpy as np
import pandas as pd

from pathlib import Path
from functools import cached_property

from pyETM.logger import get_modulelogger
from pyETM.utils import lookup_coordinates

from .region import Region
from .checks import (validate_scenario_ids, 
    validate_interconnectors, validate_mpi_profiles)

logger = get_modulelogger(__name__)


class Market:

    @property
    def name(self) -> str:
        """name of model"""
        return str(self.__name)

    @property
    def reset(self) -> bool:
        """reset on initialisation"""
        return bool(self.__reset)

    @property
    def wdir(self) -> Path:
        """working directory"""
        return self.__wdir

    @property
    def scenario_ids(self) -> dict:
        """dict with scenario_id per region"""
        return self.__scenario_ids

    @property
    def interconnectors(self) -> pd.DataFrame:
        """frame with interconnectors"""
        return self.__interconnectors.copy()

    @property
    def _regions(self) -> list[Region]:
        """list of region objects"""
        return self.__regions

    @property
    def regions(self) -> list[str]:
        """region names of region objects"""
        return [region.name for region in self._regions]

    @property
    def region_urls(self) -> dict:
        """region urls for pro environment"""
        return {str(region): region.client.pro_url for region in self._regions}

    @property
    def _interconnector_mapping(self) -> pd.DataFrame:
        """mapping of interconnectors to corresponding
        ETM interconnector keys"""
        return self.__make_mapping__()

    @property
    def interconnector_capacity(self) -> pd.Series:
        """effective interconnector capacity"""
        cols = ['p_mw', 'scaling', 'in_service']
        return self.interconnectors[cols].prod(axis=1)

    @property
    def mpi_profiles(self) -> pd.DataFrame:
        """multi purpose interconnector utilization"""
        return self.__mpi_profiles

    def __init__(self, interconnectors: pd.DataFrame, 
            scenario_ids: dict, mpi_profiles: pd.DataFrame | None = None, 
            name: str = "exchange", reset: bool = True, 
            wdir: str | Path | None = None, **kwargs):
        """initialize object"""

        # set hidden variables
        self.__name = name
        self.__reset = reset
        self.__kwargs = kwargs
        self.__wdir = Path(wdir) if wdir else Path.cwd()

        # set iterations
        self.__iterations = 0

        # log event
        logger.info("initialising exchange market '%s'", self)

        # initalize dirs to store traces
        self.__initialize_dirs__()

        # warn for non-reset
        if not reset:
            logger.critical("'%s': regions not reset on initialisation", 
                    self)

        # validate scenario ids
        scenario_ids = validate_scenario_ids(
            scenario_ids, interconnectors)

        # validate interconnectors
        interconnectors = validate_interconnectors(
            interconnectors, scenario_ids)

        # validate scenario ids again
        scenario_ids = validate_scenario_ids(
            scenario_ids, interconnectors)

        # validate mpi profiles
        mpi_profiles = validate_mpi_profiles(
            mpi_profiles, interconnectors)

        # set parameters
        self.__scenario_ids = scenario_ids
        self.__interconnectors = interconnectors
        self.__mpi_profiles = mpi_profiles

        # handle and set interconnectors, scenario ids and regions
        self.__initialize_regions__(reset=reset)

        # cache regions and market
        self._cache_regions()

        # cache expensive interconnector props
        self.available_interconnector_capacity
        self.interconnector_utilization

        # write first traces
        self._update_traces()

        # log event
        logger.debug("'%s': initialisation completed", self)

    @classmethod
    def from_excel(cls, filepath: str, name: str | None = None, **kwargs):
        """initialise market from excel"""

        # default name
        if name is None:
            name = Path(filepath).stem

        # read excel
        with pd.ExcelFile(filepath) as reader:

            # read interconnectors
            sheet, idx = 'Interconnectors', 0
            interconnectors = reader.parse(sheet, index_col=idx)

            # read scenario ids
            sheet, idx = 'Sessions', [*range(4)]
            scenario_ids = reader.parse(sheet, index_col=idx)
            
            # drop levels
            for level in ['STUDY', 'SCENARIO', 'YEAR']:
                if level in scenario_ids.index.names:
                    scenario_ids = scenario_ids.droplevel(level)

            # squeeze columns
            scenario_ids = scenario_ids.squeeze('columns')

            # read mpi profiles
            mpi_profiles = None
            if 'MPI Profiles' in reader.sheet_names:
                mpi_profiles = reader.parse('MPI Profiles')

        # initialise model
        model = cls(name=name, 
            scenario_ids=scenario_ids, mpi_profiles=mpi_profiles, 
            interconnectors=interconnectors, **kwargs)

        return model

    def __repr__(self) -> str:
        return "ExchangeModel(%s)" %self.name

    def __str__(self) -> str:
        return self.name

    def __initialize_dirs__(self):
        """initialize dirs where traces are stored"""

        # specify relevant paths
        paths = ['prices', 'utilization', 'difference', 'consistency']
        
        # iterate over paths
        for path in paths:

            # construct dirpath
            path = self.wdir.joinpath('%s/%s' %(self.name, path))
            
            # remove existing results
            if path.is_dir() & self.reset:
                shutil.rmtree(str(path))

            # create new dir
            path.mkdir(parents=True, exist_ok=True)

    def __initialize_regions__(self, reset: bool = True) -> None:
        """initialize Region classes with correct
        interconnection and scenario_id setings"""

        # new list
        regions = []

        # reference properties
        imap = self._interconnector_mapping

        # reset
        if reset:

            # get scaled capacity for etm keys
            capacities = imap.key.map(self.interconnector_capacity)

        # iterate over scenario ids dictonairy
        for region, scenario_id in self.scenario_ids.items():

            logger.info("'%s': initialising region '%s'", self, region.upper())
            
            # get interconnector names
            names = 'interconnector_' + imap.other.xs(region)

            # reset
            if reset:

                # get capacity from capacities frame
                capacity = capacities.xs(region, level=0)
                capacity.index = 'electricity_' + capacity.index + '_capacity'

            else:

                # use None
                capacity = None

            # initialze a region
            region = Region(region, scenario_id, 
                    reset=reset, capacities=capacity, 
                    interconnector_names=names, **self.__kwargs)

            # append region to self
            regions.append(region)

        # set regions
        self.__regions = regions

    def __make_mapping__(self) -> pd.DataFrame:
        """mapping of interconnector names to ETM 
        interconnector keys"""

        # reference conns
        conns = self.interconnectors

        # concat from and to regions
        series = [conns.from_region, conns.to_region]
        mapping = pd.concat(series, axis=0)

        # make midx from concated series
        names = ['region', 'key']
        arrays = [mapping.values, mapping.index]
        midx = pd.MultiIndex.from_arrays(arrays, names=names)

        # make series and enumerate icons
        mapping = pd.Series(index=midx, dtype='string')
        mapping = mapping.groupby(level=0).cumcount()

        def make_key(integer):
            """helper to construct interconnector key"""
            return f'interconnector_{integer}'

        # transform number in ETM key
        mapping = mapping.apply(lambda x: make_key(x + 1))

        # add information on other region
        series = [conns.to_region, conns.from_region]
        series = pd.concat(series, axis=0)

        # set same index as mapping
        series.index = midx

        # merge mappings
        arrays, keys = [mapping, series], ['ETM_key', 'other']
        mapping = pd.concat(arrays, axis=1, keys=keys)

        # join information with interconnectors
        mapping = conns.join(mapping, how='inner')
        mapping = mapping.reset_index(level=1).set_index('ETM_key', append=True)

        return mapping.sort_index()

    def _cache_regions(self):
        """cache relevant properties in regions"""

        # iterate over regions
        for region in self._regions:

            # log event and cache properties
            logger.info("'%s': caching region '%s'", self, region)
            region.cache_properties()

    def _get_region(self, name: str) -> Region:
        """get region object for region name"""
        return self._regions[self.regions.index(name)]

    def _update_traces(self):
        """update traces"""

        # get exchange prices
        basedir = self.wdir.joinpath(self.name)
        curves = self.electricity_prices.copy()

        for col in curves.columns:
            
            filename = basedir.joinpath('prices/%s.csv' %col)
            curves[[col]].T.to_csv(filename, index=False, mode='a',
                    header=False, sep=';', decimal=',')

        # get interconnector utilization
        curves = self.interconnector_utilization.copy()

        for col in curves.columns:
            
            filename = basedir.joinpath('utilization/%s.csv' %col)
            curves[[col]].T.to_csv(filename, index=False, mode='a',
                    header=False, sep=';', decimal=',')

        # get difference with ETM utilization
        curves = self.difference()

        for col in curves.columns:
            
            filename = basedir.joinpath('difference/%s.csv' %col)
            curves[[col]].T.to_csv(filename, index=False, mode='a',
                    header=False, sep=';', decimal=',')

        # get consistency between ETM utilizations in
        # from and to regions.
        curves = self.consistency()

        for col in curves.columns:
            
            filename = basedir.joinpath('consistency/%s.csv' %col)
            curves[[col]].T.to_csv(filename, index=False, mode='a',
                    header=False, sep=';', decimal=',')

        logger.debug("'%s': updated iteration traces", self)

    def consistency(self):
        """difference between etm utilization of 
        from and to region."""
        
        difference = []

        # reference interconnector mapping
        imap = self._interconnector_mapping
        
        # modify mapping index
        imap = imap.reset_index(level=1)
        imap = imap.set_index('key', append=True)
        imap = imap.ETM_key

        def utilization(region, interconnector):
            """get utilization"""

            # get region and utilization
            _region = self._get_region(region)
            interconnector = imap[(region, interconnector)]

            return _region.etm_utilization[interconnector]

        for conn, props in self.interconnectors.iterrows():

            # get etm utilization in both sides
            frm = utilization(props.from_region, conn)
            to = utilization(props.to_region, conn)
            
            # evaluate difference (add as oppositve signs)
            diff = frm.add(to).replace(-0.000, 0.000)

            difference.append(pd.Series(diff, name=conn))

        return pd.concat(difference, axis=1)

    def difference(self):
        """difference between utilization and etm utilization,
        returns difference based on from orient.
        
        use consistency check to see if there are differences
        in the etm utilization between the from and to region."""

        difference = []

        imap = self._interconnector_mapping

        for region in self.regions:

            # get difference
            _region = self._get_region(region)
            diff = _region.utilization - _region.etm_utilization
            
            diff = diff.round(3)
            diff = diff.replace(-0.000, 0.000)

            difference.append(diff)

        # convert to frame
        difference = pd.concat(difference, axis=1, keys=self.regions)

        cols = imap[imap.index.get_level_values('region') == imap.from_region]
        difference = difference[cols.index]

        difference.columns = difference.columns.map(imap.key)

        return difference

    @cached_property
    def interconnector_utilization(self) -> pd.DataFrame:
        """interconnector utilization"""

        # newlist
        utilization = []

        # reference interconnector mapping
        imap = self._interconnector_mapping

        # append utilization
        for region in self._regions:
            utilization.append(region.utilization)

        # convert to frame
        utilization = pd.concat(utilization, axis=1, keys=self.regions)
        
        # get correctly oriented interconnectors
        cols = imap[imap.index.get_level_values('region') == imap.from_region]
        utilization = utilization[cols.index]

        # reconstruct original columns
        utilization.columns = utilization.columns.map(imap.key)

        return utilization

    @property
    def etm_utilization(self) -> pd.DataFrame:
        """etm utilization"""

        # newlist
        utilization = []

        # reference interconnector mapping
        imap = self._interconnector_mapping

        # append utilization
        for region in self._regions:
            utilization.append(region.etm_utilization)

        # convert to frame
        utilization = pd.concat(utilization, axis=1, keys=self.regions)
        
        # get correctly oriented interconnectors
        cols = imap[imap.index.get_level_values('region') == imap.from_region]
        utilization = utilization[cols.index]

        # reconstruct original columns
        utilization.columns = utilization.columns.map(imap.key)

        return utilization

    @property
    def price_setting_units(self) -> pd.DataFrame:
        """price setting units in each region"""

        # newlist
        units = []

        # append units
        for region in self._regions:
            units.append(region.price_setting_unit)

        return pd.concat(units, axis=1, keys=self.regions)

    @property
    def price_setting_capacities(self) -> pd.DataFrame:
        """used capacity of price setting units in each region"""

        # newlist
        capacities = []

        # append capacities
        for region in self._regions:
            capacities.append(region.price_setting_capacity)

        return pd.concat(capacities, axis=1, keys=self.regions)

    @property
    def price_setting_utilization(self) -> pd.DataFrame:
        """utilization of price setting units in each region"""

        # newlist
        capacities = []

        # append capacities
        for region in self._regions:
            capacities.append(region.price_setting_utilization)

        return pd.concat(capacities, axis=1, keys=self.regions)

    @property
    def next_dispatchable_units(self) -> pd.DataFrame:
        """next dispatchable units in each region"""

        # newlist
        units = []

        # append units
        for region in self._regions:
            units.append(region.next_dispatchable_unit)

        return pd.concat(units, axis=1, keys=self.regions)

    @property
    def next_dispatchable_prices(self) -> pd.DataFrame:
        """next dispatchable unit price in each region"""

        # newlist
        prices = []

        # append prices
        for region in self._regions:
            prices.append(region.next_dispatchable_price)

        return pd.concat(prices, axis=1, keys=self.regions)

    @property
    def next_dispatchable_capacities(self) -> pd.DataFrame:
        """next dispatchable unit surplus capacity"""

        # newlist
        capacities = []

        # append capacities
        for region in self._regions:
            capacities.append(region.next_dispatchable_capacity)

        return pd.concat(capacities, axis=1, keys=self.regions)

    @property
    def electricity_prices(self) -> pd.DataFrame:
        """electricity prices in each region"""

        # newlist
        prices = []

        # append prices
        for region in self._regions:
            prices.append(region.electricity_price)

        # make frame
        frame = pd.concat(prices, axis=1, keys=self.regions)

        return frame

    @property
    def mpi_utilization(self) -> pd.DataFrame:
        """get utilization of multi purpose interconnector utilization"""
        return (self.interconnectors['mpi_perc'] / 100) * self.mpi_profiles

    @cached_property
    def available_interconnector_capacity(self) -> pd.DataFrame:
        """get remaining available capacity for the import 
        and export orients of the interconnector"""

        # reference properties
        mpi = self.mpi_utilization
        capacity = self.interconnector_capacity
        utilization = self.interconnector_utilization

        # determine exchange capacity at direction
        imp = (1.0 - mpi - utilization) * capacity
        exp = (1.0 - mpi + utilization) * capacity

        # make table
        keys = ['import', 'export']
        frame = pd.concat([imp, exp], keys=keys, axis=1)

        return frame

    @property
    def interconnector_price_deltas(self) -> pd.DataFrame:
        """get price deltas between the from and 
        to region of each interconnector"""

        # reference properties
        conns = self.interconnectors
        prices = self.electricity_prices
        dispatch = self.next_dispatchable_prices

        # make dictonairy
        frm, to = conns.from_region, conns.to_region
        mapping = dict(zip(conns.index, list(zip(frm, to))))

        # helper function
        def price_deltas(from_region, to_region):
            """Evaluated the price signals between the from and 
            to region of an interconnector.

            In case the price of the next dispatchable unit at the 
            signaled orient negates the price delta, the price delta
            is set to zero. There is no wellfare effect to be realized
            from exchange via the interconnector"""

            # determine price delta between each region
            delta = prices[from_region] - prices[to_region]
            
            # inspect if price delta also there at next dispatchable unit
            imprt = prices[from_region] - dispatch[to_region]
            exprt = dispatch[from_region] - prices[to_region]

            # aggregate result set validity of exchange
            signal = np.where(delta >= 0, imprt, exprt)
            signal = abs(delta - signal) <= abs(delta)

            # apply signal
            return (delta * signal).replace(-0.0, 0.0)

        # evaluate price deltas for combinations
        deltas = [price_deltas(frm, to) for frm, to in mapping.values()]

        return pd.concat(deltas, axis=1, keys=mapping.keys())

    @property
    def utilization_duration_curves(self) -> pd.DataFrame:
        """sort all interconnector utilization values"""

        # reference utilization
        util = self.interconnector_utilization

        # helper function
        def sort_col(col):
            """helper to sort column"""
            return util[col].sort_values(ascending=False, ignore_index=True)

        # sort columns
        curves = [sort_col(col) for col in util.columns]

        return pd.concat(curves, axis=1)

    def set_region_curves(self, utilization: pd.DataFrame, 
            prices: pd.DataFrame) -> None:
        """set interconnector availability and price curves"""

        # reference properties
        imap = self._interconnector_mapping

        # remap availability curves to ETM keys
        availability = utilization[imap.key]
        availability.columns = imap.index

        # remap price curves to ETM keys
        prices = prices[imap.other]
        prices.columns = imap.index

        # iterate over regions
        for region in self._regions:
            
            logger.info("'%s': updating region '%s'", self, region)

            # set availability curves
            # subset and set availability for region
            curves = availability.xs(region.name, axis=1, level=0)

            # determine orient based on from region
            conns = imap.xs(region.name, level=0)
            data = np.where(conns.from_region == region.name, 1, -1)

            # change availability based on conn orientation
            orient = pd.Series(data, index=conns.index)
            curves = curves.mul(orient)

            # set availability curves
            region.utilization = curves

            # subset and set prices for region
            curves = prices.xs(region.name, axis=1, level=0)
            region.exchange_prices = curves

    @property
    def iterations(self) -> int:
        return self.__iterations

    @property
    def exchange_bids(self) -> pd.DataFrame:
        """market information sheet
        
        This sheet checks if there is exchange potential between the
        from and to region on each interconnector. 
        
        This is done by looking at the import or export side of the 
        interconnector based on the price signal of each hour in the year.
        A negative price delta signals an export orient for the 
        interconnector, reasoned from the from region, and a positive price
        delta signals a import orient of the interconnector.

        When the capacity of the interconnector at the signaled exchange 
        side is not saturated, the surplus capacity is communicated. This
        capacity is always set to zero in cases where the price delta between
        the from and to region is zero, as there is no added wellfare to
        be realised from additional exchange."""

        # reference properties
        deltas = self.interconnector_price_deltas
        capacity = self.available_interconnector_capacity
        utilization = self.interconnector_utilization

        # check if import signals can be serviced
        scalar = capacity.xs('import', axis=1, level=0) > 0
        imp = deltas.where(deltas >= 0, np.nan).mul(scalar)

        # check if export signals can be serviced
        scalar = capacity.xs('export', axis=1, level=0) > 0
        exp = deltas.where(deltas < 0, np.nan).mul(scalar)

        # merge signals
        deltas = imp.fillna(exp)
        deltas = deltas.replace(-0.0, 0.0)

        # get highest exchange potential
        conns = deltas.abs().idxmax(axis=1)
        frame = conns.to_frame(name='node')

        # lookup corresponding exchange prices and utilization
        deltas = lookup_coordinates(conns, deltas)
        util = lookup_coordinates(conns, utilization)

        # get region information
        to_region = conns.map(self.interconnectors['to_region'])
        from_region = conns.map(self.interconnectors['from_region'])

        # assign importing and exporting regions
        frame['import_region'] = np.where(deltas <= 0, to_region, from_region)
        frame['export_region'] = np.where(deltas <= 0, from_region, to_region)

        # assign exchange orient and utiliztion percent
        frame['orient'] = np.where(deltas >= 0, 'import', 'export')
        frame['network_util'] = util

        # make coords for lookup of available network capacity
        coords = list(zip(frame['orient'], frame['node']))
        coords = pd.Series(coords, index=frame.index, name='orient')

        # assign available network capacity for exchange
        frame['network_mw'] = lookup_coordinates(coords, 
            self.available_interconnector_capacity)

        # lookup production surplus at exporting region
        frame['supply_mw'] = lookup_coordinates(frame['export_region'], 
            self.next_dispatchable_capacities)
        
        # lookup replacable dispatch at importing region
        frame['demand_mw'] = lookup_coordinates(frame['import_region'], 
            self.price_setting_capacities)

        # assign price delta
        frame['price_delta'] = deltas.abs().round(2)

        return frame

    @property
    def exchange_results(self) -> pd.DataFrame:
        """evaluate the updated interconnector utilization
        based on the current utilization and wellfare potential.
        
        The exchanged capacity is defined as the minimum value
        of the available exchange capacity, the surplus production
        at the exporting region and the production at the price 
        setting unit at the importing country."""

        # reference properties
        exchange = self.exchange_bids

        # determine additional exchange volume
        keys = ['network_mw', 'supply_mw', 'demand_mw']
        volume = exchange[keys].min(axis=1)

        # assign additional exchange volume
        exchange = exchange.drop(columns=keys)
        volume *= np.where(exchange['orient'] == 'import', 1, -1)

        # get full capacity
        capacity = self.interconnector_capacity
        capacity = exchange['node'].map(capacity)

        # add addtional availability percentage
        exchange['network_util'] += volume.div(capacity)
        exchange.insert(loc=4, column='exchange_mw', value=volume)

        return exchange

    @property
    def updated_interconnector_utilization(self) -> pd.DataFrame:
        """transform the market result into an updated
        interconnector utilization result for each interconnector"""

        # reference properties
        utilization = self.interconnector_utilization.copy()
        exchange = self.exchange_results

        # pivot utilization percent over interconnectors
        columns, values = 'node', 'network_util'
        util = exchange.pivot(columns=columns, values=values)

        # update utilization tables
        utilization.update(util)

        return utilization

    def clear_market(self, iterations: int = 1) -> None:
        """evalute the market and update the utilization
        of each region for each iteration"""

        for _ in range(int(iterations)):
            self._evaluate_next_iteration()

    def _evaluate_next_iteration(self) -> None:
        """evaluate next iteration of market clearing"""

        try:
            
            # set next iterations
            self.__iterations += 1

            # reference properties
            prices = self.electricity_prices
            utilization = self.updated_interconnector_utilization

            # update regions curves and cache regions
            self.set_region_curves(utilization, prices)
            self._cache_regions()

            # (re)cache available interconnector capacity
            del self.available_interconnector_capacity
            self.available_interconnector_capacity

            # (re)cache interconnector utilization
            del self.interconnector_utilization
            self.interconnector_utilization

            # update traces
            self._update_traces()

            # log event
            logger.info("completed iteration '%s' for '%s'", 
                         self.iterations, self)

        except Exception as error:

            # log event
            logger.exception("'%s': an unexpected error occured", self)
            raise error
