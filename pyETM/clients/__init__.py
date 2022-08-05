from __future__ import annotations

import logging
import requests
import asyncio
import threading

from collections.abc import Mapping
from typing import TYPE_CHECKING

from .curves import Curves
from .httplibrary import RequestsCore, AIOHTTPCore
from .parameters import Parameters
from .ccurves import CustomCurves
from .gqueries import GQueries
from .header import Header
from .interpolate import Interpolate
from .scenario import Scenario
from .merit import MeritConfiguration
from .utils import Utils

if TYPE_CHECKING:

    import ssl
    import aiohttp

    from yarl import URL

logger = logging.getLogger(__name__)

"""create a thread in which a dedicated loop 
can run as an alternative to nesting, as this
causes issues. This implementation is adapted
from https://stackoverflow.com/a/69514930"""

def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

# create thread in which a new loop can run
_LOOP = asyncio.new_event_loop()
_LOOP_THREAD = threading.Thread(target=_start_loop, 
        args=[_LOOP], daemon=True)


class BaseClient(Curves, Header, Parameters, Scenario, MeritConfiguration,
                 CustomCurves, GQueries, Interpolate, Utils):

    @classmethod
    def from_scenario_parameters(cls, end_year, area_code, metadata=None,
                                 keep_compatible=False, read_only=False, 
                                 uvalues=None, heat_network_order=None, 
                                 ccurves=None, **kwargs):
        """create new scenario from parameters on Client initalization"""
                
        # initialize new scenario
        client = cls(scenario_id=None, **kwargs)
        client.create_new_scenario(end_year, area_code, metadata=metadata, 
                                   keep_compatible=keep_compatible, 
                                   read_only=read_only)
        
        # set user values
        if uvalues is not None:
            client.user_values = uvalues
            
        # set ccurves
        if ccurves is not None:
            client.ccurves = ccurves
            
        # set heat network order
        if heat_network_order is not None:
            client.heat_network_order = heat_network_order
            
        return client
                
    @classmethod
    def from_existing_scenario(cls, scenario_id, metadata=None, 
                               keep_compatible=False, read_only=False,
                               **kwargs):
        """create new scenario as copy of existing scenario"""
        
        # initialize client
        client = cls(scenario_id=None, **kwargs)
        client.create_scenario_copy(scenario_id)

        # set scenario title
        if metadata is not None:
            client.metadata = metadata
            
        # set protection settings
        client.keep_compatible = keep_compatible
        client.read_only = read_only
                    
        return client
    
    @classmethod
    def from_saved_scenario_id(cls, scenario_id, **kwargs):
        """initialize a session with a saved scenario id"""
        
        # initialize client
        client = cls(scenario_id=None, **kwargs)
        session_id = client._get_session_id(scenario_id)
        
        return cls(session_id, **kwargs)
    
    def __str__(self):
        return f'BaseClient({self.scenario_id})'
        
    def _reset_session(self):
        """reset stored scenario properties"""
                
        # reset parameters
        self._input_values = None
        self._heat_network_order = None
        self._application_demands = None
        self._energy_flows = None
        self._production_parameters = None
        
        # reset gqueries
        self._gquery_results = None
        
        # reset ccurves
        self._ccurves = None
        
        # reset curves
        self._hourly_electricity_curves = None
        self._hourly_electricity_price_curve = None
        self._hourly_heat_curves = None
        self._hourly_household_curves = None
        self._hourly_hydrogen_curves = None
        self._hourly_methane_curves = None
        

class Client(BaseClient, RequestsCore):
        
    def __init__(self, scenario_id: int = None, beta_engine: bool = False,
            reset: bool = False, proxies: dict | None = None, 
            stream: bool = False, verify: bool | str = True, 
            cert: str | tuple | None = None):
        """Client which connects to ETM
        
        Parameters
        ----------
        scenario_id : str, default None
            The api_session_id to which the client connects. Can only access
            a limited number of methods when scenario_id is set to None.
        beta_engine : bool, default False
            Connect to the beta-engine instead of the production-engine.
        reset : bool, default False
            Reset scenario on initalization.
        proxies: dict, default None
            Dictionary mapping protocol or protocol and 
            hostname to the URL of the proxy.
        stream: boolean, default False 
            Whether to immediately download the response content.
        verify: boolean or string, default True
            Either a boolean, in which case it controls whether we verify
            the server's TLS certificate, or a string, in which case it must 
            be a path to a CA bundle to use. When set to False, requests will 
            accept any TLS certificate presented by the server, and will ignore 
            hostname mismatches and/or expired certificates, which will make 
            your application vulnerable to man-in-the-middle (MitM) attacks. 
            Setting verify to False may be useful during local development or 
            testing.
        cert: string or tuple, default None 
            If string; path to ssl client cert file (.pem). 
            If tuple; ('cert', 'key') pair.

        Returns
        -------
        self :  Client
            Object that connects to ETM API.
        """
        
        super().__init__()

        # set environment kwargs for method requests
        self._request_env = {
            "proxies": proxies, "stream": stream,
            "verify": verify, "cert": cert}

        # set session
        self._session = requests.Session()

        # set class parameters
        self.beta_engine = beta_engine
        self.scenario_id = scenario_id
        
        # set default gqueries
        self.gqueries = []
        
        # reset scenario on intialization
        if reset and (scenario_id != None):
            self.reset_scenario()
                
        # reset session?
        self._reset_session()

        logger.info("initialised: '%s'", self)

    def __repr__(self):
        return f"Client({self.scenario_id})"
        
    def __str__(self):
        return repr(self)

    def __enter__(self):
        return self
    
    def __exit__(self, *args, **kwargs):
        return None


class AsyncClient(BaseClient, AIOHTTPCore):
    
    @property
    def _loop(self):
        return _LOOP

    @property
    def _loop_thread(self):
        return _LOOP_THREAD

    def __init__(self, scenario_id: int | None = None, 
            beta_engine: bool = False, 
            reset: bool = False, proxy: str | URL | None = None, 
            proxy_auth: aiohttp.BasicAuth | None = None, 
            ssl: ssl.SSLContext | bool | aiohttp.Fingerprint | None = None, 
            proxy_headers: Mapping | None = None, 
            trust_env: bool = False):
        """Client which connects to ETM
        
        Parameters
        ----------
        scenario_id : str, default None
            The api_session_id to which the client connects. Can only access
            a limited number of methods when scenario_id is set to None.
        beta_engine : bool, default False
            Connect to the beta-engine instead of the production-engine.
        reset : bool, default False
            Reset scenario on initalization.
        proxy: str or URL, default None
            Proxy URL
        proxy_auth : aiohttp.BasicAuth, default None
            An object that represents proxy HTTP Basic authorization.
        ssl: None, False, aiohttp.Fingerprint or ssl.SSLContext, default None
            SSL validation mode. None for default SSL check 
            (ssl.create_default context() is used), False for skip 
            SSL certificate validation, aiohttp.Fingerprint for fingerprint 
            validation, ssl.SSLContext for custom SSL certificate validation. 
        proxy_headers: Mapping, default None
            HTTP headers to send to the proxy if the parameter proxy has 
            been provided.
        trust_env : bool, default False
            Should get proxies information from HTTP_PROXY / HTTPS_PROXY 
            environment variables or ~/.netrc file if present.

        Returns
        -------
        self :  Client
            Object that connects to ETM API.
        """
        
        # super class
        super().__init__()

        # set environment kwargs for session construction
        self._session_env = {
            "trust_env": trust_env}

        # set environment kwargs for method requests
        self._request_env = {
            "proxy": proxy, "proxy_auth": proxy_auth,
            "ssl": ssl, "proxy_headers": proxy_headers}

        # start loop thread if not already running
        if not self._loop_thread.is_alive():
            self._loop_thread.start()

        # set session
        self._session = None

        # set class parameters
        self.beta_engine = beta_engine
        self.scenario_id = scenario_id
        
        # set default gqueries
        self.gqueries = []
        
        # reset scenario on intialization
        if reset and (scenario_id != None):
            self.reset_scenario()
                
        # reset session?
        self._reset_session()

        logger.info("initialised: '%s'", self)

    def __repr__(self):
        return f'AsyncClient({self.scenario_id})'

    def __str__(self):
        return repr(self)

    def __enter__(self) -> AIOHTTPCore:
        """enter context manager"""

        # specify coroutine and get future
        coro = self._start_session()
        asyncio.run_coroutine_threadsafe(coro, self._loop).result()
        
        logger.debug('session_created by context manager')

        return self

    def __exit__(self, *args, **kwargs):
        """exit context manager"""

        # specify coroutine and get future
        coro = self._close_session()
        asyncio.run_coroutine_threadsafe(coro, self._loop).result()
        
        logger.debug('session destroyed by context manager')

    async def __aenter__(self) -> AIOHTTPCore:
        """enter async context manager"""
        
        # start up session
        await self._start_session()
        logger.debug('session created by async context manager')

        return self

    async def __aexit__(self, *args, **kwargs) -> None:
        """exit async context manager"""
        await self._close_session()
        logger.debug('session destroyed by async context manager')