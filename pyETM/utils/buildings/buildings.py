"""based on https://github.com/quintel/etdataset-
public/tree/master/curves/demand/buildings/space_heating by Quintel"""

from pathlib import Path
import pandas as pd

class BuildingsModel:
    """Class to describe a heating model of a building"""
    
    @classmethod
    def from_defaults(cls):
        
        # load G2A parameters
        file = Path(__file__).parent / 'data/G2A_parameters.csv'
        parameters = pd.read_csv(file)
                
        return cls(parameters)

    def __init__(self, parameters):
        
        # assign datetime to parameters
        self.parameters = self._assign_datetime_index(parameters)

    def _assign_datetime_index(self, dataframe):
        
        # make periodindex
        start = f'01-01-01 00:00'
        dataframe.index = pd.period_range(start=start, periods=8760, freq='H')
        
        return dataframe
        
    def make_heat_demand_profile(self, temperature, wind_speed):
        """
        effective temperature is defined as daily average temperature in C minus
        daily average wind speed in m/s divided by 1.5.
        """
    
        if len(temperature) != 8760:
            raise ValueError('temperature must contain 8760 values')
    
        if len(wind_speed) != 8760:
            raise ValueError('wind_speed must contain 8760 values')
    
        # merge datapoints
        profile = pd.concat([temperature, wind_speed], axis=1)
        profile.columns = ['temperature', 'wind_speed']
    
        # assign periodindex
        profile = self._assign_datetime_index(profile)
        
        # evaluate daily averages
        grouper = pd.Grouper(freq='D')
        profile = profile.groupby(grouper).mean()
        
        # evaluate effective temperature and resample hourly
        profile = profile.temperature - (profile.wind_speed / 1.5)
        profile = profile.resample('H').ffill()        
        
        # set function and merge profile and parameter data
        function = self.__calculate_profile_fraction
        profile = pd.concat([profile, self.parameters], axis=1)
        
        # evaluate and normalize profile fractions
        profile = profile.apply(lambda cols: function(*cols), axis=1)
        profile = profile / profile.sum() / 3600
        
        # set profile name
        profile.name = 'weather/buildings_heating'
        
        return profile.reset_index(drop=True)
    
    def __calculate_profile_fraction(self, Teff, TST, RER, TOP):
        """
        Calculates the profile fraction for each hour.
        If the effective temperature Teff is lower than the reference temperature
        G2A_TST, the fraction equals (G2A_TST - Teff) * G2A_RER + G2A_TOP
        Otherwise, fraction equals G2A_TOP
        """
        
        if Teff < TST:
            fraction = (TST - Teff) * RER + TOP
        else:
            fraction = TOP
        
        return fraction