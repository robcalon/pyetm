"""saved scenarios"""
from __future__ import annotations

from copy import deepcopy

import math
import pandas as pd

from pyetm.logger import get_modulelogger
from .session import SessionMethods
from .header import HeaderMethods

logger = get_modulelogger(__name__)

# consider making this seperate entity in toolbox
# as wrapper around Client class

# include updated header information

class SavedScenarioMethods(HeaderMethods, SessionMethods):
    """saved scenario related functions"""

    @property
    def saved_scenario_id(self) -> str:
        """saved scenario id"""

        if hasattr(self, '_saved_scenario_id'):
            return self._saved_scenario_id

        return None

    @saved_scenario_id.setter
    def saved_scenario_id(self, saved_scenario_id: str):

        # store previous saved scenario id
        previous = deepcopy(self.saved_scenario_id)

        # try accessing dict
        if isinstance(saved_scenario_id, dict):
            saved_scenario_id = saved_scenario_id['id']

        # convert passed id to string
        if saved_scenario_id is not None:
            saved_scenario_id = str(saved_scenario_id)

        # set new saved scenario id
        self._saved_scenario_id = saved_scenario_id

        # log changed saved scenario id
        if self.saved_scenario_id != previous:
            logger.debug(
                f"Updated saved_scenario_id: '{self.saved_scenario_id}'")

        # check if saved scenario can be fetched
        if self._saved_scenario_id is not None:
            self._get_saved_scenario(self.saved_scenario_id)

    @property
    def my_saved_scenarios(self):
        """all saved scenarios connector to account"""

        # set url
        url = 'saved_scenarios'

        # determine number of pages
        pages = self._get_scenarios(url, page=1, limit=1)
        pages = math.ceil(pages['meta']['total'] / 25)

        if pages == 0:
            return pd.DataFrame()

        # newlist
        scenarios = []
        for page in range(pages):

            # fetch pages and format scenarios
            recs = self._get_scenarios(url, page=page)['data']

            excl = ['scenario', 'scenario_id', 'scenario_id_history']
            scenarios.extend([
                self._format_scenario(scen, excl) for scen in recs])

        return pd.DataFrame.from_records(scenarios, index='id')

    def _validate_saved_scenario_id(self):
        """validate saved scenario id"""

        # check if saved scenario id is set
        if self.saved_scenario_id is None:
            raise ValueError('saved scenario id is None')

    def _get_saved_scenario(
        self,
        saved_scenario_id: str | None = None
    ) -> dict:
        """get information about scenario_ids that are
        related to the passed saved_scenario_id.

        Parameters
        ----------
        saved_scenario_id : str, default None
            Saved scenario id to which is connected

        Return
        ------
        scenario : dict
            saved scenario information"""

        # dont connect to saved scenario id
        if saved_scenario_id is None:
            saved_scenario_id = self.saved_scenario_id

        # format request
        url = f'saved_scenarios/{saved_scenario_id}'
        headers = {'content-type': 'application/json'}

        # make request
        resp = self.session.get(url, decoder='json', headers=headers)

        return resp

    # @property
    # def scenario_history(self):



    # def get_saved_scenario_history(self, saved_scenario_id: str,
    #     page: int | None = None, limit: int | None = None) -> pd.DataFrame:
    #     """get history for saved scenario"""

    #     # check permissions
    #     # self._validate_token_permission(read=True)

    #     # list scenario
    #     scenarios = self.get_saved_scenarios(page=page, limit=limit)
    #     if saved_scenario_id not in scenarios.index:
    #         raise ValueError('saved scenario not related to account')

    #     # get dictlike scenario headers
    #     scenarios = self._get_saved_scenarios(page=page, limit=limit)

    #     # subset history for scenario id
    #     history = next((
    #         scen['scenario_id_history'] for idx, scen
    #             in enumerate(scenarios['data']) if
    #                 scen['id'] == saved_scenario_id))

    #     if not history:
    #         return pd.DataFrame()

    #     # get scenario headers for history scenarios
    #     history = [BaseClient(id)._scenario_header for id in history]

    #     # transform history naar dataframe
    #     exclude = ['user_values', 'balanced_values', 'metadata', 'url']
    #     history = pd.DataFrame.from_records(history, index='id', exclude=exclude)

    #     # format datetimes
    #     history['created_at'] = history['created_at'].astype('datetime64[ns]')
    #     history['updated_at'] = history['updated_at'].astype('datetime64[ns]')

    #     return history

    def connect_to_saved_scenario(self,
        saved_scenario_id: str | None = None,
        copy: bool = True,
        metadata: dict | None = None,
        keep_compatible: bool | None = None,
        read_only: bool = False,
    ):
        """connect to a saved scenario id"""

        # connect to saved scenario id
        if saved_scenario_id is not None:
            self.saved_scenario_id = saved_scenario_id

        self.scenario_id = self._get_saved_scenario()['scenario_id']

        # create a copy
        if copy:
            self.copy_scenario(connect=True)

        # only pass if explicitly passed
        if metadata is not None:
            self.metadata = metadata

        # only set if explicitly passed
        if keep_compatible is not None:
            self.keep_compatible = keep_compatible

        # set read only parameter
        self.read_only = read_only

    def to_saved_scenario(self,
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

        # connect to saved scenario id
        if saved_scenario_id is not None:
            self.saved_scenario_id = saved_scenario_id

        # raise without saved_scenario_id
        self._validate_saved_scenario_id()

        # check if scenario id already in history
        scenario = self._get_saved_scenario()

        # raise when scenario id already in history
        if self.scenario_id in scenario['scenario_id_history']:

            msg = (
                f"scenario_id: '{self.scenario_id}' already present in "
                f"saved_scenario_history: '{saved_scenario_id}'"
            )

            raise ValueError(msg)

        # format request
        data = {'scenario_id': int(self.scenario_id)}
        headers = {'content-type': 'application/json'}
        url = f'saved_scenarios/{self.saved_scenario_id}'

        # make request
        self.session.put(url, json=data, headers=headers)

    def create_saved_scenario(
        self,
        title: str,
        private: bool,
        description: str | None = None,
        scenario_id: str | None = None,
    ) -> str:
        """create new saved scenario"""

        # self._validate_token_permission(read=True, write=True)

        # default scenario id
        if scenario_id is None:
            scenario_id = self.scenario_id

        data = {
            'title': title,
            'scenario_id': scenario_id,
            'private': str(bool(private)).lower()
        }

        if description is not None:
            data['description'] = str(description)

        # format request
        url = 'saved_scenarios'
        headers = {'content-type': 'application-json'}

        # make request
        self.saved_scenario_id = self.session.post(
            url, json=data, headers=headers)

    def delete_saved_scenario(self,
        saved_scenario_id: str | None = None, delete_scenario_ids: bool = False):
        """delete saved scenario"""

        # self._validate_token_permission(read=True, delete=True)

        # connect to saved scenario id
        if saved_scenario_id is not None:
            self.saved_scenario_id = saved_scenario_id

        # raise without saved_scenario_id
        self._validate_saved_scenario_id()

        if delete_scenario_ids is True:

            # fetch scenario history
            # delete all scenario ids

            pass

        url = f'saved_scenarios/{self.saved_scenario_id}'
        self.session.delete(url)
