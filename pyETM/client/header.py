"""scenario header"""
import functools
import pandas as pd


class Header:
    """Object to handle scenario header"""

    @property
    def scenario_header(self):
        """full scenario header"""
        return self.get_scenario_header()

    @property
    def area_code(self):
        """code for the area that the scenario describes"""
        return self.scenario_header.get('area_code')

    @property
    def created_at(self):
        """timestamp at which the scenario was created"""

        # get created at
        datetime = self.scenario_header.get('created_at')

        # format datetime
        if datetime is not None:
            datetime = pd.to_datetime(datetime, utc=True)

        return datetime

    @property
    def display_group(self):
        """?"""
        return self.scenario_header.get('display_group')

    @property
    def end_year(self):
        """target year for which the scenario is configured"""
        return self.scenario_header.get('end_year')

    @property
    def esdl_exportable(self):
        """scenario can be exported as esdl"""
        return self.scenario_header.get('esdl_exportable')

    @property
    def keep_compatible(self):
        """migrate scenario with ETM updates"""
        return self.scenario_header.get('keep_compatible')

    @keep_compatible.setter
    def keep_compatible(self, boolean):

        # format header and update
        header = {'keep_compatible': str(bool(boolean)).lower()}
        self.change_scenario_header(header)

    @property
    def metadata(self):
        """metadata tags"""
        return self.scenario_header.get('metadata')

    @metadata.setter
    def metadata(self, metadata):

        # format header and update
        header = {'metadata': dict(metadata)}
        self.change_scenario_header(header)

    @property
    def ordering(self):
        """?"""
        return self.scenario_header.get('ordering')

    @property
    def protected(self):
        """protect scenario from API changes"""
        return self.scenario_header.get('protected')

    @protected.setter
    def protected(self, boolean):

        # format header and update
        header = {'protected': str(bool(boolean)).lower()}
        self.change_scenario_header(header)

    @property
    def read_only(self):
        """set scenario to read only"""
        return self.scenario_header.get('read_only')

    @read_only.setter
    def read_only(self, boolean):

        # format header and update
        header = {'read_only': str(bool(boolean)).lower()}
        self.change_scenario_header(header)

    @property
    def scaling(self):
        """applied scaling factor"""
        return self.scenario_header.get('scaling')

    @property
    def source(self):
        """origin of the scenario"""
        return self.scenario_header.get('source')

    @property
    def start_year(self):
        """get the reference year on which the default settings are based"""
        return self.scenario_header.get('start_year')

    @property
    def template(self):
        """get id of template scenario"""
        return str(self.scenario_header.get('template'))

    @property
    def updated_at(self):
        """get timestamp of latest change"""

        # get created at
        datetime = self.scenario_header.get('updated_at')

        # format datetime
        if datetime is not None:
            datetime = pd.to_datetime(datetime, utc=True)

        return datetime

    @property
    def url(self):
        """get url"""
        return self.scenario_header.get('url')

    @property
    def pro_url(self):
        """get pro url for session id"""

        # specify base url
        base = 'https://energytransitionmodel.com'

        # update to beta server
        if self.beta_engine:
            base = base.replace('https://', 'https://beta.')

        return f'{base}/scenarios/{self.scenario_id}/load'

    @functools.lru_cache
    def get_scenario_header(self):
        """get header of scenario"""

        # return no values
        if self.scenario_id is None:
            return {}

        # raise without scenario id
        self._raise_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}'
        header = self.session.get(url)

        return header

    def change_scenario_header(self, header):
        """change header of scenario"""

        # raise without scenario id
        self._raise_scenario_id()

        # set data
        data = {"scenario": header}

        # make request
        url = f'scenarios/{self.scenario_id}'
        self.session.put(url, json=data)

        # clear scenario header cache
        self.get_scenario_header.cache_clear()
