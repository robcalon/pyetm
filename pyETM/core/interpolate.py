class Interpolate:

    def interpolate(self, ryear, connect=False):
            """Create interpolated scenario for ryear, based on the build-in
            interpolation function of the ETM-engine.
            
            This function works only for a 2050 scenario and interpolates 
            from the start year of the scenario. Use the interpolator in 
            the utils folder to interpolate between two specific scenarios.
            """

            # convert reference year to integer
            if not isinstance(ryear, int):
                ryear = int(ryear)

            # check scenario end year
            if self.end_year != 2050:
                raise NotImplementedError('Can only interpolate based on 2050 scenarios')

            # pass end year to interpolate tool
            data = {'end_year': ryear}

            # make requestd
            url = f'scenarios/{self.scenario_id}/interpolate'
            scenario = self.session.post(url, json=data)

            # get scenario id
            scenario_id = scenario['id']

            # connect to new scenario
            if connect is True:
                self.scenario_id = scenario_id

            return scenario_id
