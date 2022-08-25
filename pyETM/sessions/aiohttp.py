from __future__ import annotations

import io
import json
import logging
import asyncio
import pandas as pd

from urllib.parse import urljoin
from collections.abc import Mapping
from typing import TYPE_CHECKING

from pyETM.types import Decoder, Method
from pyETM.exceptions import UnprossesableEntityError
from pyETM.optional import import_optional_dependency

from .utils.loop import _LOOP, _LOOP_THREAD

if TYPE_CHECKING:

    import ssl
    import aiohttp

    from yarl import URL

logger = logging.getLogger(__name__)


class AIOHTTPSession:

    @property
    def loop(self):
        return _LOOP

    @property
    def loop_thread(self):
        return _LOOP_THREAD

    @property
    def base_url(self) -> str | None:
        return self.__base_url

    @base_url.setter
    def base_url(self, base_url: str | None) -> None:

        if base_url is None:
            self.__base_url = base_url

        if base_url is not None:
            self.__base_url = str(base_url)

    def __init__(self, proxy: str | URL | None = None, 
        proxy_auth: aiohttp.BasicAuth | None = None, 
        ssl: ssl.SSLContext | bool | aiohttp.Fingerprint | None = None, 
        proxy_headers: Mapping | None = None, 
        trust_env: bool = False):
        """session object for pyETM clients
        
        Parameters
        ----------
        base_url: str, default None
            Base url to which the session connects, all request urls
            will be merged with the base url to create a destination. 
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
            environment variables or ~/.netrc file if present."""

        # set environment kwargs for session construction
        self._session_env = {
            "trust_env": trust_env}

        # set environment kwargs for method requests
        self._request_env = {
            "proxy": proxy, "proxy_auth": proxy_auth,
            "ssl": ssl, "proxy_headers": proxy_headers}

        # start loop thread if not already running
        if not self.loop_thread.is_alive():
            self.loop_thread.start()

        # set session
        self._session = None

        # start loop thread if not already running
        if not self.loop_thread.is_alive():
            self.loop_thread.start()

    def __repr__(self):
        return f'AIOHTTPSession(base_url={self.base_url})'

    def __str__(self):
        return repr(self)

    def __enter__(self):
        """enter context manager"""
        
        # connect
        self.connect()
        logger.debug('session_created by context manager')

        return self

    def __exit__(self):
        """exit context manager"""

        self.close()
        logger.debug('session destroyed by context manager')

    def make_url(self, url: str | None = None):
        """join url with base url"""
        return urljoin(self.base_url, url)

    def connect(self):
        """connect session"""

        # specify coroutine and get future
        coro = self.async_connect()
        asyncio.run_coroutine_threadsafe(coro, self.loop).result()
        
        return self

    def close(self, *args, **kwargs):
        """close session"""

        # specify coroutine and get future
        coro = self.async_close()
        asyncio.run_coroutine_threadsafe(coro, self.loop).result()

    def request(self, method: Method, url: str, 
            decoder: Decoder = "bytes", **kwargs):
        """request and handle api response"""

        # specify coroutine and get future
        coro = self.async_request(method, url, decoder, **kwargs)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)

        return future.result()
            
    def delete(self, url: str | None = None, 
            decoder: Decoder = 'text', **kwargs):
        return self.request("delete", self.make_url(url), decoder, **kwargs)

    def get(self, url: str | None = None, 
            decoder: Decoder = 'json', **kwargs):
        return self.request("get", self.make_url(url), decoder, **kwargs)
            
    def post(self, url: str | None = None, 
            decoder: Decoder = 'json', **kwargs):
        return self.request("post", self.make_url(url), decoder, **kwargs)

    def put(self, url: str | None = None, 
            decoder: Decoder = 'json', **kwargs):
        return self.request("put", self.make_url(url), decoder, **kwargs)

    def upload_series(self, url: str | None = None, 
            series: pd.Series | None = None, 
            name: str | None = None, **kwargs):
        """upload series object"""
        
        if TYPE_CHECKING:
            # import aiohttp
            import aiohttp

        else:
            # optional module import
            aiohttp = import_optional_dependency('aiohttp')

        # default to empty series
        if series is None:
            series = pd.Series()

        # set key as name
        if name is None:
            name = "not specified"

        # convert values to string
        data = series.to_string(index=False)
        
        # insert data in form
        form = aiohttp.FormData()
        form.add_field("file", data, filename=name)

        return self.put(url, data=form, **kwargs)

    async def __aenter__(self):
        """enter async context manager"""
        
        # start up session
        await self.async_connect()
        logger.debug('session created by async context manager')

        return self

    async def __aexit__(self, *args, **kwargs) -> None:
        """exit async context manager"""

        await self.async_close()
        logger.debug('session destroyed by async context manager')

    async def async_connect(self):
        """connect session"""
        
        if TYPE_CHECKING:
            # import aiohttp
            import aiohttp

        else:
            # optional module import
            aiohttp = import_optional_dependency('aiohttp')

        self._session = aiohttp.ClientSession(**self._session_env)

    async def async_close(self):
        """close session"""

        # close and remove session
        await self._session.close()
        self._session = None      
          
    async def async_request(self, method: Method, url: str, 
            decoder: Decoder = 'bytes', **kwargs):
        """make request to api session"""

        if TYPE_CHECKING:
            # import aiohttp
            import aiohttp

        else:
            # optional module import
            aiohttp = import_optional_dependency('aiohttp')

        retries = 5
        while retries:

            try:

                # merge kwargs with session envioronment kwargs
                kwargs = {**self._request_env, **kwargs}
                context = bool(self._session)

                # create context
                if not context:
                    await self.start_session()
                    logger.debug('single use session created')

                # make method request
                request = getattr(self._session, method)
                async with request(url, **kwargs) as resp:

                    # check response
                    if not (resp.status <= 400):
                                                
                        # report error messages
                        if resp.status == 422:
                            self._error_report(resp)
                        
                        # raise for status
                        resp.raise_for_status()

                    # bytes decoding
                    if decoder == "bytes":
                        resp = await resp.read()

                    # bytes as BytesIO
                    elif decoder == "BytesIO":
                        byts = await resp.read()
                        resp = io.BytesIO(byts)

                    # json decoding
                    elif decoder == "json":
                        resp = await resp.json(encoding="utf-8")
                    
                    # text decoding
                    elif decoder == "text":
                        resp = await resp.text(encoding="utf-8")

                    else:
                        msg = "decoding method '%s' not implemented" %method
                        raise NotImplementedError(msg)

                    # logger.debug("processed '%s' request with '%s' decoder", 
                    #         method, decoder)

                    return resp

            # except connectionerrors and retry
            except aiohttp.ClientConnectorError as error:
                retries -= 1

                # raise after retries
                if not retries:
                    raise error

            finally:
                
                # close non contextual session
                if not context:
                    await self.close_session()
                    logger.debug('single use session destroyed')

    async def _error_report(self, resp: aiohttp.ClientResponse) -> None:
        """create error report when api returns error messages."""

        try:

            # attempt decode error message(s)
            msg = await resp.json(encoding="utf-8")
            errors = msg.get("errors")
            
        except json.decoder.JSONDecodeError:
            
            # no message returned
            errors = None
            
        # trigger special raise
        if errors:
            
            # create error report
            base = "ETEngine returned the following error(s):"
            msg = """%s\n > {}""".format("\n > ".join(errors)) %base

            raise UnprossesableEntityError(msg)
