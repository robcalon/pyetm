"""based on https://github.com/quintel/etdataset-
public/tree/master/curves/demand/households/space_heating by Quintel"""

from pathlib import Path
import pandas as pd

from .smoothing import ProfileSmoother


class HouseholdProfileModel:
    """Class to describe a heating model of a house"""
    
    @property
    def u_value(self):
        return 1 / self.r_value
    
    @property
    def concrete_mass(self):
        return self.surface_area * self.wall_thickness * self.p_concrete
    
    @property
    def heat_capacity(self):
        return self.concrete_mass * self.c_concrete / 3.6e6
            
    @property
    def exchange_delta(self):
        return self.u_value * self.surface_area / 1000    
    
    @classmethod
    def from_defaults(cls, house_type, insulation_type, **kwargs):
        """Quintel default values"""
        
        # load properties
        file = Path(__file__).parent / 'data/house_properties.csv'
        properties = pd.read_csv(file, index_col=[0, 1])
        
        # subset correct house and insulation profile
        properties = properties.loc[(house_type, insulation_type)]

        # load thermostat values
        file = Path(__file__).parent / 'data/thermostat_values.csv'
        thermostat = pd.read_csv(file, usecols=[insulation_type]).squeeze('columns')
                
        # convert to dictonairy
        config = properties.to_dict()
        
        # append other config props
        config['thermostat'] = thermostat.to_dict()
        config['house_type'] = house_type
        config['insulation_type'] = insulation_type
            
        # append kwargs
        config.update(kwargs)
        
        return cls(**config)
    
    def __init__(self, behaviour, surface_area, thermostat, r_value, 
                 wall_thickness, window_area, house_type, insulation_type, 
                 **kwargs):
        """kwargs are passed to smoother"""
        
        # set constants
        self.p_concrete = 2400
        self.c_concrete = 880        
        
        self.behaviour = behaviour
        self.surface_area = surface_area
        self.window_area = window_area

        self.thermostat = thermostat
        self.inside_temperature = self.thermostat[0]
        
        self.r_value = r_value
        self.wall_thickness = wall_thickness
        
        self.house_type = house_type
        self.insulation_type = insulation_type
        
        self.__smoother = ProfileSmoother(**kwargs)
    
    def check_profile(self, profile):
            
        # check profile length
        if len(profile) != 8760:
            raise ValueError(f'"{profile.name}" must contain 8760 values')
    
        return profile
        
    def make_heat_demand_profile(self, temperature, irradiation):
        """heat demand profile"""
        
        if len(temperature) != 8760:
            raise ValueError('temperature must contain 8760 values')
        
        if len(irradiation) != 8760:
            raise ValueError('irradiation must contain 8760 values')

        # merge datapoints
        profile = pd.concat([temperature, irradiation], axis=1)
        profile.columns = ['temperature', 'irradiance']
    
        # make periodindex
        start = f'01-01-01 00:00'
        profile.index = pd.period_range(start=start, periods=8760, freq='H')
        
        # set hour columns
        profile['hour'] = profile.index.hour
        
        # calculate heat demand
        func = self._calculate_heat_demand
        profile = profile.apply(lambda cols: func(*cols), axis=1)
                
        # smooth profile
        func = self.__smoother.calculate_smoothed_demand
        profile = func(profile.values, self.insulation_type)

        # name profile
        name = f'weather/insulation_{self.house_type}_{self.insulation_type}'
        profile = pd.Series(profile, name=name, dtype='float64')
                
        # factor profile
        profile = profile / profile.sum() / 3600
            
        return profile
    
    def _calculate_heat_demand(self, outside_temperature, 
                              solar_irradiation, hour_of_the_day):
        
        thermostat_temperature = self.thermostat[hour_of_the_day]
        
        # How much energy is needed from heating to bridge the temperature gap?
        if self.inside_temperature < thermostat_temperature:

            needed_heating_demand = (
                (thermostat_temperature - self.inside_temperature) * 
                self.heat_capacity
            )
        
        else:
            needed_heating_demand = 0.0

        # Updating the inside temperature
        if self.inside_temperature < thermostat_temperature:
            self.inside_temperature = thermostat_temperature

        # How big is the difference between the temperature inside and outside?
        temperature_difference = self.inside_temperature - outside_temperature
        
        # How much heat is leaking away in this hour?
        energy_leaking = (
            self.exchange_delta * temperature_difference
        )
        
        # How much energy is added by irradiation?
        energy_added_by_irradiation = solar_irradiation * self.window_area
            
        # What is the inside temperature after the leaking?
        self.inside_temperature = (
            self.inside_temperature - 
            (energy_leaking - energy_added_by_irradiation) / 
            self.heat_capacity
        )
        
        return needed_heating_demand


class HouseholdsModel:
    """Class to create household profiles"""

    @classmethod
    def from_defaults(cls):
        """Quintel default values"""
        
        # load properties
        cols = ['house_type', 'insulation_level']
        file = Path(__file__).parent / 'data/house_properties.csv'
        properties = pd.read_csv(file, usecols=cols)
        
        house_types = properties.house_type.unique()
        insulation_types = properties.insulation_level.unique()
        
        return cls(house_types, insulation_types)
            
    def __init__(self, house_types, insulation_types):
        
        # set arguments
        self.house_types = house_types
        self.insulation_types = insulation_types
                    
    def make_heat_demand_profile(self, house_type, insulation_type,
                                  temperature, irradiance, **kwargs):
        """kwargs are passed to smoother"""
        
        model = HouseholdProfileModel.from_defaults(house_type, insulation_type, **kwargs)
        profile =  model.make_heat_demand_profile(temperature, irradiance)
    
        return profile
    
    def make_heat_demand_profiles(self, temperature, irradiance, **kwargs):
        """kwargs are passed to smoother"""
        
        # reference props
        houses = self.house_types
        levels = self.insulation_types
        
        # set up parameters
        func = self.make_heat_demand_profile
        config = {'temperature' : temperature, 'irradiance': irradiance}
        
        # append kwargs
        config.update(kwargs)
        
        # make profiles
        profiles = [func(h, l, **config) for h in houses for l in levels]
        
        return pd.concat(profiles, axis=1)