import functools
import pandas as pd


class Sankey:

    @property
    def sankey(self):
        return self.get_sankey()

    @functools.lru_cache
    def get_sankey(self):
        """get the sankey data"""

        # raise without scenario id
        self._raise_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}/sankey'
        resp = self.session.get(url, decoder="BytesIO")

        # convert to frame
        sankey = pd.read_csv(resp, index_col=['Group', 'Carrier', 'Category', 'Type'])

        return sankey
