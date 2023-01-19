"""saved scenarios"""
from __future__ import annotations

import pandas as pd

from .session import SessionMethods
from .base import BaseClient


class SavedScenarioMethods(SessionMethods):
    """saved scenario related functions"""

    def get_saved_scenario_id(self, saved_scenario_id: str):
        """get information about scenario_ids that are
        related to the passed saved_scenario_id.

        Parameters
        ----------
        saved_scenario_id : int or str
            Saved scenario id to which is connected

        Return
        ------
        scenario : dict
            saved scenario information"""

        # raise without scenario id or required permission
        # self._validate_token_permission(read=True)

        # format request
        url = f'saved_scenarios/{saved_scenario_id}'
        headers = {'content-type': 'application/json'}

        # make request
        resp = self.session.get(url, decoder='json', headers=headers)

        return resp

    def _get_saved_scenarios(self,
        page: int | None = None, limit: int | None = None) -> dict:
        """Get saved scenarios info connected to token"""

        # raise without scenario id or required permission
        # self._validate_token_permission(read=True)

        # newdict
        params = {}

        # add page
        if page is not None:
            params['page'] = int(page)

        # add limit
        if limit is not None:
            params['limit'] = int(limit)

        # format request
        url = 'saved_scenarios'
        headers = {'content-type': 'application/json'}

        # request response
        resp = self.session.get(
            url, params=params, decoder='json', headers=headers)

        return resp

    def get_saved_scenarios(self,
        page: int | None = None, limit: int | None = None) -> pd.DataFrame:
        """Get saved scenarios connected to token.

        Parameters
        ----------
        page : int, default None
            The page number to fetch
        limit : int, default None
            The number of items per page

        Return
        ------
        saved_scenarios : pd.DataFrame
            DataFrame that lists saved scenarios
            connected to token."""

        # subset data part
        scenarios = self._get_saved_scenarios(page=page, limit=limit)
        scenarios = scenarios.get('data')

        def reduce_scenario(scenario):
            """helper function to reduce scenario"""
            return {k:v for k,v in scenario.items() if k != 'scenario'}

        # get scenario infor and merge to frame
        scenarios = [reduce_scenario(scen) for scen in scenarios]
        scenarios = pd.DataFrame.from_records(scenarios, index='id')

        # format data
        scenarios['created_at'].astype('datetime64[ns]')
        scenarios['updated_at'].astype('datetime64[ns]')

        # handle owner
        owner = scenarios['owner']
        owner = pd.DataFrame.from_records(owner.values, index=owner.index)
        owner = owner.add_prefix('owner_')

        # replace owner and drop history
        keys = ['owner', 'scenario_id_history']
        scenarios = scenarios.drop(keys, axis=1)
        scenarios = pd.concat([scenarios, owner], axis=1)

        # rename index
        scenarios.index.name = None

        return scenarios

    def get_saved_scenario_history(self, saved_scenario_id: str,
        page: int | None = None, limit: int | None = None) -> pd.DataFrame:
        """get history for saved scenario"""

        # check permissions
        # self._validate_token_permission(read=True)

        # list scenario
        scenarios = self.get_saved_scenarios(page=page, limit=limit)
        if saved_scenario_id not in scenarios.index:
            raise ValueError('saved scenario not related to account')

        # get dictlike scenario headers
        scenarios = self._get_saved_scenarios(page=page, limit=limit)

        # subset history for scenario id
        history = next((
            scen['scenario_id_history'] for idx, scen
                in enumerate(scenarios['data']) if
                    scen['id'] == saved_scenario_id))

        if not history:
            return pd.DataFrame()

        # get scenario headers for history scenarios
        history = [BaseClient(id)._scenario_header for id in history]

        # transform history naar dataframe
        exclude = ['user_values', 'balanced_values', 'metadata', 'url']
        history = pd.DataFrame.from_records(history, index='id', exclude=exclude)

        # format datetimes
        history['created_at'] = history['created_at'].astype('datetime64[ns]')
        history['updated_at'] = history['updated_at'].astype('datetime64[ns]')

        return history

    def to_saved_scenario_id(self,
        saved_scenario_id: str | None = None):
        """if none than use present saved_scenario_id, otherwise create
        new saved scenario id. when specified uses passed id

        Parameters
        ----------
        saved_scenario_id : str, default None
            The saved scenario id to which to connect.
        """

        # raise without scenario id
        self._validate_scenario_id()

        # check permissions
        # self._validate_token_permission(read=True)

        # check if scenario id already in history
        scenario = self.get_saved_scenario_id(saved_scenario_id)

        # raise when already connected to latest scenario
        if self.scenario_id == scenario['scenario_id']:

            msg = (
                f"scenario_id: '{self.scenario_id}' already saved in "
                f"saved_scenario: '{saved_scenario_id}'"
            )

            raise ValueError(msg)

        # raise when scenario id already in history
        if self.scenario_id in scenario['scenario_id_history']:

            msg = (
                "scenario_id: '{self.scenario_id}' already present in "
                f"saved_scenario_history: '{saved_scenario_id}'"
            )

            raise ValueError(msg)

        # format request
        url = f'saved_scenarios/{saved_scenario_id}'
        data = {'scenario_id': self.scenario_id}
        headers = {'content-type': 'application-json'}

        # make request
        self.session.put(url, json=data, headers=headers)
