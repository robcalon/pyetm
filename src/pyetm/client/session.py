"""Base methods and client"""
from __future__ import annotations

import copy
import functools
import os
import re
from typing import Any

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.sessions.abc import SessionABC
from pyetm.types import TokenScope, Endpoint

# get modulelogger
logger = get_modulelogger(__name__)


class SessionMethods:
    """Core methods for API interaction"""

    @property
    def _default_engine_url(self) -> str:
        """default engine url"""
        return "https://engine.energytransitionmodel.com/api/v3/"

    @property
    def connected_to_default_engine(self) -> bool:
        """connected to default engine url?"""
        return self.engine_url == self._default_engine_url

    @property
    def _scenario_header(self) -> dict[str, Any]:
        """get full scenario header"""
        return self._get_scenario_header()

    @property
    def engine_url(self):
        """engine URL"""
        return self._engine_url

    @engine_url.setter
    def engine_url(self, url: str | None):
        # default url
        if url is None:
            url = self._default_engine_url

        # set engine
        self._engine_url = str(url)

        # reset token and change base url
        self.token = None

        # reset cache
        self._reset_cache()

    @property
    def etm_url(self):
        """model URL"""

        # raise error
        if self._etm_url is None:
            raise ValueError("ETModel URL not set on initialisation.")

        return self._etm_url

    @etm_url.setter
    def etm_url(self, url: str | None):
        # use default pro location
        if (url is None) & (self.connected_to_default_engine):
            url = "https://energytransitionmodel.com/"

        # set etmodel
        self._etm_url = str(url)

    @property
    def scenario_id(self) -> int | None:
        """scenario id"""
        return self._scenario_id if hasattr(self, "_scenario_id") else None

    @scenario_id.setter
    def scenario_id(self, scenario_id: int | None):
        # store previous scenario id
        previous = copy.deepcopy(self.scenario_id)

        # try accessing dict
        if isinstance(scenario_id, dict):
            scenario_id = scenario_id["id"]

        # convert passed id to integer
        if scenario_id is not None:
            scenario_id = int(scenario_id)

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

    def make_endpoint_url(self, endpoint: Endpoint, extra: str = "") -> str:
        """The url of the API endpoint for the connected scenario"""

        if endpoint == "curves":
            # validate merit order
            self._validate_merit_order()

            return self.make_endpoint_url(
                endpoint="scenario_id", extra=f"curves/{extra}"
            )

        if endpoint == "custom_curves":
            return self.make_endpoint_url(
                endpoint="scenario_id", extra=f"custom_curves/{extra}"
            )

        if endpoint == "inputs":
            return self.make_endpoint_url(
                endpoint="scenario_id", extra=f"inputs/{extra}"
            )

        if endpoint == "saved_scenarios":
            return self.session.make_url(
                self.engine_url, url=f"saved_scenarios/{extra}"
            )

        if endpoint == "scenario_id":
            # validate scenario id
            self._validate_scenario_id()

            return self.session.make_url(
                self.engine_url, url=f"scenarios/{self.scenario_id}/{extra}"
            )

        if endpoint == "scenarios":
            return self.session.make_url(self.engine_url, "scenarios")

        if endpoint == "token":
            return self.session.make_url(self.engine_url, url="/oauth/token/info")

        if endpoint == "transition_paths":
            return self.session.make_url(self.engine_url, url="transition_paths")

        if endpoint == "user":
            # validate token permission
            self._validate_token_permission("openid")

            return self.session.make_url(self.engine_url, url="oauth/userinfo")

        raise NotImplementedError(f"endpoint not implemented: '{endpoint}'")

    def to_etm_url(self, load: bool = False):
        """make url to access scenario in etm gui"""

        # raise without scenario id
        self._validate_scenario_id()

        # relative path
        url = f"scenarios/{self.scenario_id}"

        # append load path
        if load is True:
            url = f"{url}/load"

        return self.session.make_url(self.etm_url, url)

    @property
    def token(self):
        """optional personal access token for authorized use"""

        # return None without token
        if self._token is None:
            return None

        # request parameters
        url = self.make_endpoint_url(endpoint="token")
        headers = {"content-type": "application/json"}

        # make request
        token = self.session.get(url, headers=headers, content_type="application/json")

        # convert utc timestamps
        token["created_at"] = pd.to_datetime(token["created_at"], unit="s")

        # convert experiation delta
        if isinstance(token["expires_in"], int):
            token["expires_in"] = pd.Timedelta(token["expires_in"], unit="s")

        return pd.Series(token, name="token")

    @token.setter
    def token(self, token: str | None = None):
        # check environment variables for token
        if token is None:
            token = os.getenv("ETM_ACCESS_TOKEN")

        # store token
        self._token = token

        # update persistent session headers
        if self._token is None:
            # pop authorization if present
            if "Authorization" in self.session.headers.keys():
                self.session.headers.pop("Authorization")

        else:
            # set authorization
            authorization = {"Authorization": f"Bearer {self._token}"}
            self.session.headers.update(authorization)

    @property
    def user(self) -> pd.Series:
        """info about token owner if token assigned"""

        # request parameters`
        url = self.make_endpoint_url(endpoint="user")
        headers = {"content-type": "application/json"}

        # make request
        user = self.session.get(url, headers=headers, content_type="application/json")

        return pd.Series(user, name="user")

    @property
    def session(self) -> SessionABC:
        """set object that handles requests"""
        return self._session

    @session.setter
    def session(self, session: SessionABC) -> None:
        self._session = session

    @functools.lru_cache(maxsize=1)
    def _get_scenario_header(self) -> dict[str, Any]:
        """get header of scenario"""

        # return no values
        if self.scenario_id is None:
            return {}

        # request parameters
        url = self.make_endpoint_url(endpoint="scenario_id")
        headers = {"content-type": "application/json"}

        # make request
        header = self.session.get(url, headers=headers, content_type="application/json")

        return header

    def _get_session_id(self) -> int:
        """get a session_id for a pro-environment scenario"""

        # make url
        url = self.to_etm_url(load=True)

        # make request
        content = self.session.get(url, content_type="text/html")

        # get session id from response
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        # handle non-match
        if session_id is None:
            raise ValueError(f"Failed to scrape api_session_id from URL: '{url}'")

        return int(session_id.group(1))

    def _validate_scenario_id(self):
        """raise error when scenario id is None"""

        # check if scenario id is None
        if self.scenario_id is None:
            raise ValueError("scenario id is None")

    def _validate_token_permission(self, scope: TokenScope = "public"):
        """validate token permission"""

        # raise without token
        if self.token is None:
            raise ValueError("No personall access token asssigned")

        # check if scope is known
        if scope is None:
            raise ValueError(f"Unknown token scope: '{scope}'")

        # validate token scope
        if scope not in self.token.loc["scope"]:
            raise ValueError(f"Token has no '{scope}' permission.")

    @property
    def merit_order_enabled(self) -> bool:
        """see if merit order is enabled"""

        # target input parameter
        key = "settings_enable_merit_order"

        # prepare request
        headers = {"content-type": "application/json"}
        url = self.make_endpoint_url(endpoint="inputs", extra=key)

        # make request
        parameter = self.session.get(
            url, headers=headers, content_type="application/json"
        )

        # get relevant setting
        enabled = parameter.get("user", parameter["default"])

        # check for iterpolation issues
        if not (enabled == 1) | (enabled == 0):
            raise ValueError(f"invalid setting: '{key}'={enabled}")

        return bool(enabled)

    # @merit_order_enabled.setter
    # def merit_order_enabled(self, boolean: bool):

    #     # target input parameter
    #     key = "settings_enable_merit_order"

    def _validate_merit_order(self):
        """check if merit order is enabled"""

        # raise for disabled merit order
        if self.merit_order_enabled is False:
            raise ValueError(f"{self}: merit order disabled")

    def _reset_cache(self):
        """reset cached scenario properties"""

        # clear parameter caches
        self._get_scenario_header.cache_clear()

    def _update_scenario_header(self, header: dict):
        """change header of scenario"""

        # set data
        data = {"scenario": header}
        url = self.make_endpoint_url(endpoint="scenario_id")

        # make request
        self.session.put(url, json=data)

        # clear scenario header cache
        self._get_scenario_header.cache_clear()
