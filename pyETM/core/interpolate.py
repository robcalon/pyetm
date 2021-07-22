class Interpolate:

    def interpolate(self, ryear, connect=False):
            """create interpolated scenario for ryear"""

            if isinstance(ryear, str):
                ryear = int(ryear)

            if not isinstance(ryear, int):
                raise TypeError()

            # check scenario end year
            if self.end_year != 2050:
                raise NotImplementedError('Can only interpolate based on 2050 scenarios')

            # pass end year to interpolate tool
            data = {'end_year': ryear}

            # prepare post
            headers = {'Connection': 'close'}
            post = f'/scenarios/{self.scenario_id}/interpolate'

            # get interpolated scenario id
            scenario = self.post(post, json=data, headers=headers)
            scenario_id = scenario['id']

            # connect to new scenario
            if connect is True:
                self.scenario_id = scenario_id

            return scenario_id