"""Base methods and client"""
from __future__ import annotations
from typing import Literal

import os
import re
import copy
import functools

from pyetm.logger import get_modulelogger
from pyetm.sessions import RequestsSession, AIOHTTPSession

# get modulelogger
logger = get_modulelogger(__name__)

SCOPE = Literal['public', 'read', 'write', 'delete']


class SessionMethods:
    """Core methods for API interaction"""

    @property
    def _scenario_header(self) -> dict:
        """get full scenario header"""
        return self._get_scenario_header()

    @property
    def base_url(self) -> str:
        """"base url for carbon transition model"""

        # return beta engine url
        if self.beta_engine:
            return "https://beta-engine.energytransitionmodel.com/api/v3/"

        return "https://engine.energytransitionmodel.com/api/v3/"

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
    def token(self) -> dict | None:
        """optional personal access token for authorized use"""

        # return None without token
        if self._token is None:
            return None

        # url = '/oauth/userinfo'
        url = '/oauth/token/info'
        headers = {'content-type': 'application/json'}

        # get token information
        resp: dict = self.session.get(
            url, decoder='json', headers=headers)

        return resp

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
        if self._token is None:

            # pop authorization if present
            if 'Authorization' in self.session.headers.keys():
                self.session.headers.pop('Authorization')

        else:

            # set authorization
            authorization = {'Authorization': f'Bearer {self._token}'}
            self.session.headers.update(authorization)

    @property
    def session(self) -> RequestsSession | AIOHTTPSession:
        """session object that handles requests"""
        return self._session if hasattr(self, '_session') else None

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

    def _validate_scenario_id(self):
        """raise error when scenario id is None"""

        # check if scenario id is None
        if self.scenario_id is None:
            raise ValueError('scenario id is None')

    def _validate_token_permission(self, scope: SCOPE = 'public'):
        """validate token permission"""

        # default is public
        if self._token is None:
            raise ValueError('No personal access token assigned.')

        # check read permission
        elif scope == 'read':
            if not 'scenarios:read' in self.token.get('scope'):
                raise ValueError("Token has no 'read' permission")

        # check write permission
        elif scope == 'write':
            if not 'scenarios:write' in self.token.get('scope'):
                raise ValueError("Token has no 'write' permission")

        # check delete permission
        elif scope == 'delete':
            if not 'scenarios:delete' in self.token.get('scope'):
                raise ValueError("Token has no 'delete' permission")

        else:
            # not (yet) implemented
            raise NotImplementedError(
                f"Valiation for scope '{scope}' not implemented")

    def _reset_cache(self):
        """reset cached scenario properties"""

        # clear parameter caches
        self._get_scenario_header.cache_clear()

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
