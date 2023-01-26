"""base functionality for client"""
from __future__ import annotations

import os
import re
import copy
import functools

from pyETM.logger import get_modulelogger
from pyETM.sessions import RequestsSession, AIOHTTPSession

# get modulelogger
logger = get_modulelogger(__name__)


class SessionMethods:
    """session methods"""

    @property
    def beta_engine(self) -> bool:
        """connects to beta-engine when True and to production-engine
        when False."""
        return self._beta_engine

    @beta_engine.setter
    def beta_engine(self, boolean: bool) -> None:
        """set beta engine attribute"""

        # set boolean and reset session
        self._beta_engine = bool(boolean)

        # set related settings
        self.token = None
        self.session.base_url = self.base_url

        self._reset_cache()

    @property
    def base_url(self) -> str:
        """"base url for carbon transition model"""

        # return beta engine url
        if self.beta_engine:
            return "https://beta-engine.energytransitionmodel.com/api/v3/"

        # return production engine url
        return "https://engine.energytransitionmodel.com/api/v3/"

    @property
    def scenario_id(self) -> str | None:
        """scenario id"""
        return self._scenario_id if hasattr(self, '_scenario_id') else None

    @scenario_id.setter
    def scenario_id(self, scenario_id: str | None):

        # store previous scenario id
        previous = copy.deepcopy(self.scenario_id)

        # try accessing dict
        if isinstance(scenario_id, dict):
            scenario_id = scenario_id['id']

        # convert passed id to string
        if scenario_id is not None:
            scenario_id = str(scenario_id)

        # set new scenario id
        self._scenario_id = scenario_id

        # log changed scenario id
        if self.scenario_id != previous:
            logger.debug(f"Updated scenario_id: '{self.scenario_id}'")

        # reset session
        if self.scenario_id != previous:
            self._reset_cache()

        # validate scenario id
        self._get_scenario_header()

    @property
    def session(self) -> RequestsSession | AIOHTTPSession:
        """session object that handles requests"""
        return self._session if hasattr(self, '_session') else None

    @property
    def token(self) -> str | None:
        """optional personal access token for authorized use"""
        return self._token

    @token.setter
    def token(self, token: str | None = None):

        # check environment variables for production token
        if (token is None) & (not self.beta_engine):
            token = os.getenv('ETM_ACCESS_TOKEN')

        # check environment variables for beta token
        if (token is None) & self.beta_engine:
            token = os.getenv('ETM_BETA_ACCESS_TOKEN')

        # store token
        self._token = token

        # update persistent session headers
        if self.token is None:

            # pop authorization if present
            if 'Authorization' in self.session.headers.keys():
                self.session.headers.pop('Authorization')

        else:

            # set authorization
            authorization = {'Authorization': f'Bearer {self.token}'}
            self.session.headers.update(authorization)

    def _get_session_id(self, scenario_id: str) -> str:
        """get a session_id for a pro-environment scenario"""

        # make pro url
        host = "https://energytransitionmodel.com"
        url = f"{host}/saved_scenarios/{scenario_id}/load"

        # extract content from url
        content = self.session.request("get", url, decoder='text')

        # get session id from content
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        return session_id.group(1)

    @functools.lru_cache(maxsize=1)
    def _get_scenario_header(self):
        """get header of scenario"""

        # return no values
        if self.scenario_id is None:
            return {}

        # raise without scenario id
        self._validate_scenario_id()

        # make request
        url = f'scenarios/{self.scenario_id}'
        header = self.session.get(url)

        return header

    def _validate_scenario_id(self):
        """raise error when scenario id is None"""

        # check if scenario id is None
        if self.scenario_id is None:
            raise ValueError('scenario id is None')

    def _validate_token_permission(self, read: bool, write: bool):
        """check if token has correct authorization"""

    def _reset_cache(self):
        """reset cached scenario properties"""

        # clear parameter caches
        self._get_scenario_header.cache_clear()

    def copy_scenario(self,
        scenario_id: str | None = None, connect: bool = True) -> int:
        """Create a new scenario that is a copy of an existing scenario
        based on its id. The client automatically connects to the the
        copied scenario when connect is True.

        Parameters
        ----------
        scenario_id : str, default None
            The scenario_id that is copied. Defaults
            to own scenario_id.
        connect : bool, default True
            Connect to the copied scenario_id

        Return
        ------
        scenario_id : int
            The scenario_id of the copied scenario."""

        # use own scenario id
        if scenario_id is None:

            # raise without scenario id
            self._validate_scenario_id()
            scenario_id = self.scenario_id

        # make and set scenario
        scenario = {'scenario_id': str(scenario_id)}
        data = {"scenario": scenario}

        # request response
        url = 'scenarios'
        resp = self.session.post(url, json=data)

        # connect to scenario_id
        if connect:
            self.scenario_id = str(resp['id'])

        return str(resp['id'])

    def create_new_scenario(self,
        area_code: str, end_year: int, metadata: dict | None = None,
        keep_compatible: bool = False, read_only: bool = False) -> None:
        """Create a new scenario on the ETM server.

        Parameters
        ----------
        area_code : str
            Area code of the created scenario
        end_year : int
            End year of the created scenario
        metadata : dict, default None
            metadata
        keep_compatible, bool, default False
            keep
        read_only : bool, default False
            read only"""

        # default scenario
        if isinstance(end_year, str):
            end_year = int(end_year)

        # make scenario dict based on args
        scenario = {'end_year': end_year, 'area_code' : area_code}

        # set metadata
        if metadata is not None:
            scenario['metadata'] = metadata

        # set protection settings
        scenario['keep_compatible'] = keep_compatible
        scenario['read_only'] = read_only

        # default scenario
        if scenario is None:
            scenario = {}

        # set scenario parameter
        data = {"scenario": scenario}

        # make request
        url = 'scenarios'
        response = self.session.post(url, json=data)

        # update scenario_id
        self.scenario_id = str(response['id'])

    def delete_scenario(self, scenario_id: str | None = None) -> None:
        """Delete scenario"""

        # validate token
        # self._validate_token_permission(read=True, delete=True)

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

    def reset_scenario(self) -> None:
        """Resets user values and heat network order
        to default settings."""

        # set reset parameter
        data = {"reset": True}
        url = f'scenarios/{self.scenario_id}'

        # make request
        self.session.put(url, json=data)

        # reinitialize connected scenario
        self._reset_cache()
