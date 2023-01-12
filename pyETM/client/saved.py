from __future__ import annotations

class SavedScenario:

    def get_saved_scenarios(self,
        page: int | None = None, limit: int | None = None):
        """Get saved scenarios connected to token

        Parameters
        ----------
        page : int, default None
            The page number to fetch
        limit : int, default None
            The number of items per page

        Returns
        -------
        saved_scenarios : pd.DataFrame
            Dataframe that lists all saved scenarios
        """

        # raise without scenario id or required permission
        # self._raise_token_permission(read=True)

        params = {}

        if page is not None:
            params['page'] = int(page)

        if limit is not None:
            params['limit'] = int(limit)

        # request response
        url = 'saved_scenarios'
        resp = self.session.get(url, params=params, decoder='json')

        return resp

