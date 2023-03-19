
from __future__ import annotations
from collections.abc import Iterable

import math
import pandas as pd

from pyetm.logger import get_modulelogger
from .session import SessionMethods

logger = get_modulelogger(__name__)


class AccountMethods(SessionMethods):
    """Authentication methods"""

    @property
    def my_scenarios(self):
        """ all scenarios connected to account"""

        # set url
        url = 'scenarios'

        # determine number of pages
        pages = self._get_scenarios(url=url, page=1, limit=1)
        pages = math.ceil(pages['meta']['total'] / 25)

        if pages == 0:
            return pd.DataFrame()

        # newlist
        scenarios = []
        for page in range(pages):

            # fetch pages and format scenarios
            recs = self._get_scenarios(url, page=page)['data']

            excl = ['user_values', 'balanced_values', 'metadata', 'url']
            scenarios.extend([
                self._format_scenario(scen, excl) for scen in recs])

        return pd.DataFrame.from_records(scenarios, index='id')

    def _format_scenario(self, scenario, exclude: Iterable | None = None):
        """helper function to reformat a saved scenario"""

        # default list
        if exclude is None:
            exclude = []

        # add string to list
        if isinstance(exclude, str):
            exclude = [exclude]

        # flatten passed keys
        for key in ['owner']:
            if key in scenario:

                # flatten items in dict
                item = scenario.pop(key)
                item = {f'{key}_{k}': v for k, v in item.items()}

                # add back to scenario
                scenario = {**scenario, **item}

        # process datetimes
        for key in ['created_at', 'updated_at']:
            if key in scenario:
                if scenario.get(key) is not None:
                    scenario[key] = pd.to_datetime(scenario[key], utc=True)

        for key in ['template']:
            if key in scenario:
                if scenario.get(key) is None:
                    scenario[key] = pd.NA

        # reduce items in scenario
        return {k:v for k,v in scenario.items() if k not in exclude}

    def _get_scenarios(self, url: str,
        page: int = 1, limit: int = 25) -> dict:
        """Get saved scenarios info connected to token"""

        # raise without scenario id or required permission
        self._validate_token_permission(scope='read')

        # format request
        params = {'page': int(page), 'limit': int(limit)}
        headers = {'content-type': 'application/json'}

        # request response
        resp = self.session.get(
            url, params=params, decoder='json', headers=headers)

        return resp
