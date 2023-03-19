"""scenario header"""
from __future__ import annotations

import copy
import pandas as pd

from .session import SessionMethods

class HeaderMethods(SessionMethods):
    """additional header methods"""

    @property
    def _scenario_header(self) -> dict:
        """get full scenario header"""
        return self._get_scenario_header()

    @property
    def area_code(self) -> str:
        """code for the area that the scenario describes"""
        return self._scenario_header.get('area_code')

    @property
    def created_at(self) -> pd.Timestamp:
        """timestamp at which the scenario was created"""

        # get created at
        datetime = self._scenario_header.get('created_at')

        # format datetime
        if datetime is not None:
            datetime = pd.to_datetime(datetime, utc=True)

        return datetime

    @property
    def end_year(self) -> int:
        """target year for which the scenario is configured"""
        return self._scenario_header.get('end_year')

    @property
    def esdl_exportable(self):
        """scenario can be exported as esdl"""
        return self._scenario_header.get('esdl_exportable')

    @property
    def keep_compatible(self) -> bool:
        """migrate scenario with ETM updates"""
        return self._scenario_header.get('keep_compatible')

    @keep_compatible.setter
    def keep_compatible(self, boolean: bool):

        # format header and update
        header = {'keep_compatible': str(bool(boolean)).lower()}
        self._update_scenario_header(header)

    @property
    def metadata(self) -> dict:
        """metadata tags"""
        return self._scenario_header.get('metadata')

    @metadata.setter
    def metadata(self, metadata: dict):

        # format header and update
        header = {'metadata': dict(metadata)}
        self._update_scenario_header(header)

    @property
    def owner(self) -> dict:
        """scenario owner if created by logged in user"""
        return self._scenario_header.get("owner")

    @property
    def private(self) -> bool:
        """boolean that determines if the scenario is private"""
        return self._scenario_header.get('private')

    @private.setter
    def private(self, boolean: bool):

        # format header and update
        header = {'private': str(bool(boolean)).lower()}
        self._update_scenario_header(header)

    @property
    def protected(self) -> bool:
        """protect scenario from API changes"""
        return self._scenario_header.get('protected')

    @protected.setter
    def protected(self, boolean: bool):

        # format header and update
        header = {'protected': str(bool(boolean)).lower()}
        self._update_scenario_header(header)

    @property
    def pro_url(self) -> str:
        """get pro url for session id"""

        # specify base url
        base = 'https://energytransitionmodel.com'

        # update to beta server
        if self.beta_engine:
            base = base.replace('https://', 'https://beta.')

        return f'{base}/scenarios/{self.scenario_id}/load'

    @property
    def scaling(self):
        """applied scaling factor"""
        return self._scenario_header.get('scaling')

    @property
    def source(self):
        """origin of the scenario"""
        return self._scenario_header.get('source')

    @property
    def start_year(self) -> int:
        """get the reference year on which the default settings are based"""
        return self._scenario_header.get('start_year')

    @property
    def template(self) -> int | None:
        """the id of the scenario that was used as a template,
        or None if no template was used."""
        return str(self._scenario_header.get('template'))

    @property
    def updated_at(self) -> pd.Timestamp:
        """get timestamp of latest change"""

        # get created at
        datetime = self._scenario_header.get('updated_at')

        # format datetime
        if datetime is not None:
            datetime = pd.to_datetime(datetime, utc=True)

        return datetime

    @property
    def url(self) -> str:
        """get url"""
        return self._scenario_header.get('url')

    def _update_scenario_header(self, header: dict):
        """change header of scenario"""

        # raise without scenario id
        self._validate_scenario_id()

        # set data
        data = {"scenario": header}
        url = f'scenarios/{self.scenario_id}'

        # make request
        self.session.put(url, json=data)

        # clear scenario header cache
        self._get_scenario_header.cache_clear()

    def add_metadata(self, metadata: dict):
        """append metadata"""

        original = copy.deepcopy(self.metadata)
        self.metadata = {**original, **metadata}
