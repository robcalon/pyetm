import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class MeritConfiguration:
    
    def _get_merit_configuration(self):
        """get merit configuration JSON
        
        TO DO
        -----
        - should be able to make calls with and without curves,
        pending implementation on API
        - consider property decorator for merit configuration."""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection': 'close'}
        url = f'/scenarios/{self.scenario_id}/merit'
        
        # request response
        resp = self.get(url, headers=headers)

        return resp
    
    def get_participants(self, subset=None):
        """get particpants from merit configuration"""
        
        # supported subtypes
        supported =  ['total_consumption', 'with_curve', 'generic', 
                      'storage', 'dispatchable', 'must_run', 'volatile']

        # subset all types
        if subset is None:
            subset = supported

        # subset consumer types
        elif (subset == 'consumer') | (subset == 'consumers'):
            subset = ['total_consumption', 'with_curve']

        # subset flexible types
        elif (subset == 'flexible') | (subset == 'flexibles'):
            subset = ['generic', 'storage']

        # subset producer types
        elif (subset == 'producer') | (subset == 'producers'):
            subset = ['dispatchable', 'must_run', 'volatile']

        # other keys always in list
        if isinstance(subset, str):
            subset = [subset]

        # convert non list-like to list
        if not isinstance(subset, list):
            subset = list(subset)
        
        # get and correct response JSON
        recs = self._get_merit_configuration()['participants']
        recs = [rec for rec in recs if rec.get('type') in subset]
        
        def correct(rec):
            """null correction in recordings"""
            return {k: (v if (v != 'null') & (v is not None) else np.nan) 
                    for k, v in rec.items()}
        
        # correct records to replace null with None
        recs = [correct(rec) for rec in recs]
        frame = pd.DataFrame.from_records(recs, index='key')

        # format records
        frame = frame.rename_axis(None, axis=0).sort_index()
        frame = frame.drop(columns='curve')

        return frame
        
    def get_participant_curves(self):
        """get curves for each participant"""
        
        # get participants JSON
        response = self._get_merit_configuration()
        
        # map participants to curve names
        # drops paricipants without curve
        recs = response['participants']
        cmap = {rec['key']: rec['curve'] for rec in recs if rec['curve']}

        # extract curves from response
        curves = response['curves']
        curves = pd.DataFrame.from_dict(curves)

        def sset_column(key, value):
            """helper to rename subsetted column"""
            return pd.Series(curves[value], name=key)

        # subset curve for each partipant key
        curves = [sset_column(k, v) for k, v in cmap.items()]
        curves = pd.concat(curves, axis=1)

        return curves.sort_index(axis=1)

    def get_next_dispatchable_unit(self):
        """returns the name, volume and price of 
        the dispatchable to supply an addtional
        unit of energy"""

        # fetch participants and match ecurves
        ladder = self.__make_dispatchables_bidladder()
        ladder.index += '.output (MW)'

        # fetch and subset relevant ecurves
        ecurves = self.hourly_electricity_curves
        ecurves = ecurves[ladder.index]

        # find standby unit based on utilization
        # round to prevent merit/python rounding errors
        util = ecurves.div(ladder.capacity).round(2)
        standby = (util < 1).idxmax(axis=1)

        # map relevant properties
        installed = standby.map(ladder.capacity)
        dispatched = self._lookup_coords(standby, ecurves)

        # evalaute available capacity and price
        # round to prevent merit/python rounding erros
        available = (installed - dispatched).round(2)
        prices = standby.map(ladder.marginal_costs).round(2)

        # get relevant series and column names
        keys = ['unit', 'volume', 'price']
        series = [standby, available, prices]

        # make and correct frame
        frame = pd.concat(series, axis=1, keys=keys)
        frame = self.__correct_saturated_hours(frame, ladder.marginal_costs)

        # reconstruct original index convention
        frame.unit = frame.unit.str.rstrip(".output (MW)")

        return frame

    def __correct_saturated_hours(self, frame, bidladder):
        """correction for cases where all dispatchables
        are saturated and the idxmax function returns 
        the first item on the bidladder"""

        # specify where arguments
        key, price = bidladder.idxmax(), bidladder.max()
        condition = (frame.volume != 0.0)

        # correct special cases
        frame.unit = frame.unit.where(condition, key)
        frame.price = frame.price.where(condition, price)

        return frame

    def __make_dispatchables_bidladder(self):
        """make a bidladder for subset dispatchables.
        returns both marginal costs and installed capacity"""

        # get all dispatchable units
        units = self.get_participants(subset='dispatchable')

        # remove interconnectors
        pattern = "energy_interconnector_\d?\d_imported_electricity"
        units = units[~units.index.str.contains(pattern, regex=True)]

        # cap related keys
        k1 = 'availability'
        k2 = 'number_of_units'
        k3 = 'output_capacity_per_unit'

        # evalaute capacity and specify relevant columns
        units['capacity'] = units[[k1, k2, k3]].product(axis=1)
        units = units[['marginal_costs', 'capacity']]

        return units.sort_values(by='marginal_costs')

    def _lookup_coords(self, coords, frame, **kwargs):
        """lookup function to get coordinate values from dataframe"""

        # reindex frame with factorized coords
        idx, cols = pd.factorize(coords)
        values = frame.reindex(cols, axis=1)

        # lookup values
        values = values.to_numpy()
        values = values[np.arange(len(values)), idx]

        return pd.Series(values, index=frame.index, **kwargs)