"""account"""
from __future__ import annotations

import math
from collections.abc import Iterable

import pandas as pd

from pyetm.logger import get_modulelogger

from .session import SessionMethods

logger = get_modulelogger(__name__)


class AccountMethods(SessionMethods):
    """Account object methods"""

    @property
    def my_scenarios(self) -> pd.DataFrame:
        """all scenarios connected to account"""

        # validate token permission
        self._validate_token_permission("scenarios:read")

        # set url
        url = self.make_endpoint_url(endpoint="scenarios")

        # determine number of pages
        pages = self._get_objects(url=url, page=1, limit=1)
        pages = math.ceil(pages["meta"]["total"] / 100)

        if pages == 0:
            return pd.DataFrame()

        # newlist
        scenarios = []
        for page in range(1, pages + 1):
            # fetch pages and format scenarios
            recs = self._get_objects(url, page=page, limit=100)["data"]

            excl = ["user_values", "balanced_values", "metadata", "url"]
            scenarios.extend([self._format_object(scen, excl) for scen in recs])

        return pd.DataFrame.from_records(scenarios, index="id")

    @property
    def my_saved_scenarios(self) -> pd.DataFrame:
        """all saved scenarios connector to account"""

        # validate token permission
        self._validate_token_permission("scenarios:read")

        # set url
        url = self.make_endpoint_url(endpoint="saved_scenarios")

        # determine number of pages
        pages = self._get_objects(url, page=1, limit=1)
        pages = math.ceil(pages["meta"]["total"] / 100)

        if pages == 0:
            return pd.DataFrame()

        # newlist
        scenarios = []
        for page in range(1, pages + 1):
            # fetch pages and format scenarios
            recs = self._get_objects(url, page=page, limit=100)["data"]

            excl = ["scenario", "scenario_id", "scenario_id_history"]
            scenarios.extend([self._format_object(scen, excl) for scen in recs])

        return pd.DataFrame.from_records(scenarios, index="id")

    @property
    def my_transition_paths(self) -> pd.DataFrame:
        """all transition paths connected to account"""

        # validate token permission
        self._validate_token_permission("scenarios:read")

        # set url
        url = self.make_endpoint_url(endpoint="transition_paths")

        # determine number of pages
        pages = self._get_objects(url, page=1, limit=1)
        pages = math.ceil(pages["meta"]["total"] / 100)

        if pages == 0:
            return pd.DataFrame()

        # newlist
        paths = []
        for page in range(1, pages + 1):
            # fetch pages and format scenarios
            recs = self._get_objects(url, page=page, limit=100)["data"]
            paths.extend([self._format_object(path) for path in recs])

        return pd.DataFrame.from_records(paths, index="id")

    def _format_object(self, obj: dict, exclude: Iterable | None = None):
        """helper function to reformat a object."""

        # default list
        if exclude is None:
            exclude = []

        # add string to list
        if isinstance(exclude, str):
            exclude = [exclude]

        # flatten passed keys
        for key in ["owner"]:
            if key in obj:
                # flatten items in dict
                item = obj.pop(key)
                item = {f"{key}_{k}": v for k, v in item.items()}

                # add back to scenario
                obj = {**obj, **item}
        # process datetimes
        for key in ["created_at", "updated_at"]:
            if key in obj:
                if obj.get(key) is not None:
                    obj[key] = pd.to_datetime(obj[key], utc=True)

        for key in ["template"]:
            if key in obj:
                if obj.get(key) is None:
                    obj[key] = pd.NA

        # reduce items in scenario
        return {k: v for k, v in obj.items() if k not in exclude}

    def _get_objects(self, url: str, page: int = 1, limit: int = 25):
        """Get info about object in url that are connected
        to the user token. Object can be scenarios, saved scenarios
        or transition paths."""

        # raise without required permission
        self._validate_token_permission(scope="scenarios:read")

        # format request
        params = {"page": int(page), "limit": int(limit)}
        headers = {"content-type": "application/json"}

        # request response
        objects = self.session.get(
            url, params=params, content_type="application/json", headers=headers
        )

        return objects

    def _get_saved_scenario_id(self, saved_scenario_id: int) -> int:
        """get latest scenario id for saved scenario"""

        # make url
        url = self.make_endpoint_url(
            endpoint="saved_scenarios", extra=str(saved_scenario_id)
        )

        # get most recent scenario id
        scenario = self.session.get(url, content_type="application/json")

        return int(scenario["scenario_id"])
