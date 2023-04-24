"""Authentication methods"""

from __future__ import annotations
# from pathlib import Path

import copy

import pandas as pd
from .session import SessionMethods


class ScenarioMethods(SessionMethods):
    """Base scenario methods"""

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

        # # validate token permission
        self._validate_token_permission(scope='scenarios:write')

        # format header and update
        header = {'private': str(bool(boolean)).lower()}
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

    def add_metadata(self, metadata: dict):
        """append metadata"""

        original = copy.deepcopy(self.metadata)
        self.metadata = {**original, **metadata}

    def copy_scenario(
        self,
        scenario_id: str | None = None,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        private: bool | None = None,
        connect: bool = True
    ) -> int:
        """Create a new scenario that is a copy of an existing scenario
        based on its id. The client automatically connects to the the
        copied scenario when connect is True.

        Parameters
        ----------
        scenario_id : str, default None
            The scenario_id that is copied. Defaults
            to own scenario_id.
        metadata : dict, default None
            Metadata passed to the scenario. Inherits the
            metadata of the copied scenario id by default.
        keep_compatible : bool, default None
            Keep scenario compatible with future
            versions of ETM. Inherits the keep compatible
            setting of the copied scenario id by default.
        private : bool, default None
            Make the scenario private. Inherits the privacy
            setting of the copied scenario id by default.
        connect : bool, default True
            Connect to the copied scenario id.

        Return
        ------
        scenario_id : int
            The scenario_id of the copied scenario."""

        # use own scenario id
        if scenario_id is None:

            # raise without scenario id
            self._validate_scenario_id()
            scenario_id = self.scenario_id

        # remember original scenario id.
        previous = copy.deepcopy(self.scenario_id)

        # make and set scenario
        scenario = {'scenario_id': str(scenario_id)}
        data = {"scenario": scenario}

        # request scenario id and connect
        url = 'scenarios'
        self.scenario_id = self.session.post(
            url, json=data)['id']

        # set metadata parmater
        if metadata is not None:
            self.metadata = metadata

        # set compatability parameter
        if keep_compatible is not None:
            self.keep_compatible = keep_compatible

        # set private parameter
        if private is not None:
            self.private = private

        # revert to original scenario_id
        if connect is False:
            self.scenario_id = previous

        return scenario_id

    def create_new_scenario(
        self,
        area_code: str,
        end_year: int,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        private: bool | None = None
    ) -> None:
        """Create a new scenario on the ETM server.

        Parameters
        ----------
        area_code : str
            Area code of the created scenario
        end_year : int
            End year of the created scenario
        metadata : dict, default None
            metadata passed to scenario.
        keep_compatible : bool, default None
            Keep scenario compatible with future
            versions of ETM. Defaults to settings
            in original scenario.
        private : bool, default None
            Make the scenario private."""

        # default scenario
        if isinstance(end_year, str):
            end_year = int(end_year)

        # make scenario dict based on args
        scenario = {'end_year': end_year, 'area_code' : area_code}

        # set scenario parameter
        data = {"scenario": scenario}

        # make request
        url = 'scenarios'
        response = self.session.post(url, json=data)

        # update scenario_id
        self.scenario_id = str(response['id'])

        # set scenario metadata
        if metadata is not None:
            self.metadata = metadata

        # set keep compatible parameter
        if keep_compatible is not None:
            self.keep_compatible = keep_compatible

        # should validate permission
        if private is not None:
            self.private = private

    def delete_scenario(self, scenario_id: str | None = None) -> None:
        """Delete scenario"""

        # validate token
        self._validate_token_permission(scope='scenarios:delete')

        # use connected scenario
        previous = None
        if (scenario_id is not None) & ((str(scenario_id)) != self.scenario_id):

            # remember original connected scenario
            # and connect to passed scenario id
            previous = copy.deepcopy(self.scenario_id)
            self.scenario_id = scenario_id

        # delete scenario
        url = f'scenarios/{self.scenario_id}'
        self.session.delete(url=url)

        # connect to previous or None
        self.scenario_id = previous

    def interpolate_scenario(self, ryear: int, connect: bool = False):
        """Create interpolated scenario for ryear, based on the build-in
        interpolation function of the ETM-engine.

        This function works only for a 2050 scenario and interpolates
        from the start year of the scenario. Use the interpolator in
        the utils folder to interpolate between two specific scenarios."""

        # convert reference year to integer
        if not isinstance(ryear, int):
            ryear = int(ryear)

        # check scenario end year
        if self.end_year != 2050:
            raise NotImplementedError(
                'Can only interpolate based on 2050 scenarios')

        # pass end year to interpolate tool
        data = {'end_year': ryear}

        # make requestd
        url = f'scenarios/{self.scenario_id}/interpolate'
        scenario = self.session.post(url, json=data, decoder='json')

        # get scenario id
        scenario_id = scenario['id']

        # connect to new scenario
        if connect is True:
            self.scenario_id = scenario_id

        return scenario_id

    def reset_scenario(self) -> None:
        """Resets user values, heat network order
        and forecast storage order to default settings."""

        # set reset parameter
        data = {"reset": True}
        url = f'scenarios/{self.scenario_id}'

        # make request
        self.session.put(url, json=data)

        # reinitialize connected scenario
        self._reset_cache()

    def to_saved_scenario(
        self,
        saved_scenario_id: str | None = None,
        title: str | None = None,
        description: str | None = None,
        private: bool | None = None,
    ) -> int:
        """Save scenario to a saved scenario id.

        Parameters
        ----------
        saved_scenario_id : str, default None
            The saved scenario id at which to save the scenario id.
            Creates a new saved scenario id when None is passed.
        title : str, default None
            The name of the saved scenario when a new saved scenario
            is created. Default to 'API Generated - <scenario_id>'.
        description : str, default None
            The description of the saved scenario when a new saved
            scenario is created. Default to add no description.
        private : bool, default None
            Make the created saved scenario private. Uses the user
            account configed setting by default.

        Return
        ------
        saved_scenario_id : int
            The (created) saved scenario id is returend."""

        # raise without scenario id and validate permission
        self._validate_scenario_id()
        self._validate_token_permission("scenarios:write")

        # prepare request
        url = 'saved_scenarios'
        headers = {'content-type': 'application/json'}

        # make data
        data = {"scenario_id": self.copy_scenario(connect=False)}

        # update exisiting saved scenario
        if saved_scenario_id is not None:

            # update url
            url += f'/{saved_scenario_id}'

            return self.session.put(
                url, json=data, headers=headers)

        # default title
        if title is None:
            title = f'API Generated - {self.scenario_id}'

        # add title
        data["title"] = title

        # add privacy setting
        if private is not None:
            data["private"]: str(bool(private)).lower()

        # add description
        if description is not None:
            data["description"] = str(description)

        return self.session.post(
            url, json=data, headers=headers)

    # def to_dict(self): #, path: str | Path) -> None:
    #     """export full scenario to dict"""
