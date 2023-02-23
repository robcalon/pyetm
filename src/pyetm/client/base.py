"""base client object"""
from __future__ import annotations

from pyetm.logger import get_modulelogger
from pyetm.sessions import RequestsSession, AIOHTTPSession
from .header import HeaderMethods

# get modulelogger
logger = get_modulelogger(__name__)


class BaseClient(HeaderMethods):
    """Base Client"""

    def __init__(self,
        scenario_id: str | None = None, beta_engine: bool = False,
        reset: bool = False, token: str | None = None,
        session: RequestsSession | AIOHTTPSession | None = None,
        **kwargs):
        """client object to process ETM requests via its public API

        Parameters
        ----------
        scenario_id : str, default None
            The api_session_id to which the client connects. Can only access
            a limited number of methods when scenario_id is set to None.
        beta_engine : bool, default False
            Connect to the beta-engine instead of the production-engine.
        reset : bool, default False
            Reset scenario on initalization.
        token : str, default None
            Personal access token to authenticate requests to your
            personal account and scenarios. Detects token automatically
            from environment when assigned to ETM_ACCESS_TOKEN when
            connected to production or ETM_BETA_ACCESS_TOKEN when
            connected to beta.
        session: object instance, default None
            session instance that handles requests to ETM's public API.
            Default to use a RequestsSession.

        All key-word arguments are passed directly to the init method
        of the default session instance. All key-word arguments are
        ignored when the session argument is not None.

        Returns
        -------
        self : Client
            Object that processes ETM requests via public API"""

        super().__init__()

        # default session
        if session is None:
            session = RequestsSession(**kwargs)

        # set session
        self.__kwargs = kwargs
        self._session = session

        # set engine and token
        self.beta_engine = beta_engine
        self.token = token

        # set scenario id
        self.scenario_id = scenario_id

        # set default gqueries
        self.gqueries = []

        # reset scenario on intialization
        if reset and (scenario_id is not None):
            self.reset_scenario()

        # make message
        msg = (
            "Initialised new Client: "
                f"'scenario_id={self.scenario_id}, "
                f"area_code={self.area_code}, "
                f"end_year={self.end_year}'"
        )

        logger.debug(msg)

    def __enter__(self):
        """enter conext manager"""

        # connect session
        self.session.connect()

        return self

    def __exit__(self, *args, **kwargs):
        """exit context manager"""

        # close session
        self.session.close()

    def __repr__(self):
        """reproduction string"""

        # get initialization parameters
        params = {
            **{
                "scenario_id": self.scenario_id,
                "beta_engine": self.beta_engine,
                "token": self.token,
                "session": self.session
                },
            **self.__kwargs
            }

        # object environment
        env = ", ".join(f'{k}={v}' for k, v in params.items())

        return f"BaseClient({env})"

    def __str__(self):
        """stringname"""

        # make stringname
        strname = (
            "BaseClient("
                f"scenario_id={self.scenario_id}, "
                f"area_code={self.area_code}, "
                f"end_year={self.end_year})"
        )

        return strname
