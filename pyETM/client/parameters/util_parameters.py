class UtilParameters:
    
    @property
    def _cvalues(self):
        """continous user values"""
        
        # get relevant parameters
        keys = self._dvalues.index
        cvalues = self.scenario_parameters

        # get continious parameters
        cvalues = cvalues[~cvalues.index.isin(keys)]
    
        return cvalues.astype('float64')
    
    @property
    def _dvalues(self):
        """discrete user values"""
        
        """
        Work in progress
        ----------------
        Needs change in inputs JSON to identify discrete values by
        the permitted_values column. Using fixed keys to identify them
        for now.
        
        Update self._check_user_value_values as well once this issue has 
        been fixed. 
        """
        
        keys = [
            'heat_storage_enabled',
            'merit_order_subtype_of_energy_power_nuclear_uranium_oxide',
            'settings_enable_merit_order',
            'settings_enable_storage_optimisation_energy_flexibility_hv_opac_electricity',
            'settings_enable_storage_optimisation_energy_flexibility_pumped_storage_electricity',
            'settings_enable_storage_optimisation_energy_flexibility_mv_batteries_electricity',
            'settings_enable_storage_optimisation_energy_flexibility_flow_batteries_electricity',
            'settings_enable_storage_optimisation_transport_car_flexibility_p2p_electricity',
            'settings_weather_curve_set',
        ]
        
        # get discrete parameters
        dvalues = self.scenario_parameters
        dvalues = dvalues[dvalues.index.isin(keys)]
        
        return dvalues