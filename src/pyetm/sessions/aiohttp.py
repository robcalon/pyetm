"""aiohttp session object"""
from __future__ import annotations

from io import BytesIO
from typing import Any, Literal, Mapping, overload, TYPE_CHECKING

import asyncio
import pandas as pd

from pyetm.optional import import_optional_dependency
from pyetm.sessions.abc import SessionTemplate
from pyetm.types import ContentType, Method
from pyetm.utils.loop import _loop, _loop_thread

if TYPE_CHECKING:
    from yarl import URL
    from ssl import SSLContext
    from aiohttp import ClientSession, FormData, Fingerprint, BasicAuth


class AIOHTTPSession(SessionTemplate):
    """aiohttps based adaptation"""

    @property
    def loop(self):
        """used event loop"""
        return _loop

    @property
    def loop_thread(self):
        """seperate thread for event loop"""
        return _loop_thread

    def __init__(
        self,
        proxy: str | URL | None = None,
        proxy_auth: BasicAuth | None = None,
        ssl: SSLContext | bool | Fingerprint | None = None,
        proxy_headers: Mapping | None = None,
        trust_env: bool = False,
    ):
        """session object for pyETM clients

        Parameters
        ----------
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
        self.context = {"trust_env": trust_env}

        # set environment kwargs for method requests
        self.kwargs = {
            "proxy": proxy,
            "proxy_auth": proxy_auth,
            "ssl": ssl,
            "proxy_headers": proxy_headers,
        }

        # start loop thread if not already running
        if not self.loop_thread.is_alive():
            self.loop_thread.start()

        # # set session
        self._session: ClientSession | None = None

    async def __aenter__(self):
        """enter async context manager"""

        # start up session
        await self.connect_async()
        return self

    async def __aexit__(self, *args, **kwargs):
        """exit async context manager"""
        await self.close_async()

    def connect(self):
        """sync wrapper for async session connect"""

        # specify coroutine and get future
        coro = self.connect_async()
        asyncio.run_coroutine_threadsafe(coro, self.loop).result()

        return self

    async def connect_async(self):
        """async session connect"""

        if not TYPE_CHECKING:
            ClientSession = import_optional_dependency('aiohttp.ClientSession')

        self._session = ClientSession(**self.context)

    def close(self):
        """sync wrapper for async session close"""

        # specify coroutine and get future
        coro = self.close_async()
        asyncio.run_coroutine_threadsafe(coro, self.loop).result()

    async def close_async(self):
        """async session close"""

        # await session close
        if self._session is not None:
            await self._session.close()

        # reset session
        self._session = None

    def upload(
        self,
        url: str,
        series: pd.Series,
        filename: str | None = None,
    ) -> dict[str, Any]:
        """upload series"""

        # optional module import
        aiohttp = import_optional_dependency("aiohttp")

        # set key as name
        if filename is None:
            filename = "filename not specified"

        # convert series to string
        data = series.to_string(index=False)

        # insert data in form
        form: FormData = aiohttp.FormData()
        form.add_field("file", data, filename=filename)

        return self.request(
            method="put", url=url, content_type="application/json", data=form
        )

    @overload
    def request(
        self,
        method: Method,
        url: str,
        content_type: Literal["application/json"],
        **kwargs,
    ) -> dict[str, Any]:
        pass

    @overload
    def request(
        self,
        method: Method,
        url: str,
        content_type: Literal["text/csv"],
        **kwargs,
    ) -> BytesIO:
        pass

    @overload
    def request(
        self,
        method: Method,
        url: str,
        content_type: Literal["text/html"],
        **kwargs,
    ) -> str:
        pass

    def request(
        self,
        method: Method,
        url: str,
        content_type: ContentType,
        **kwargs,
    ) -> dict[str, Any] | BytesIO | str:
        """make request to api session"""

        # specify coroutine and get future
        coro = self.make_async_request(method, url, content_type, **kwargs)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)

        return future.result()

    async def make_async_request(
        self,
        method: Method,
        url: str,
        content_type: ContentType,
        **kwargs,
    ):
        """make request to api session"""

        # reusable existing session
        session = bool(self._session)

        # create session
        if not session:
            await self.connect_async()

        # merge base and request headers
        headers = kwargs.get("headers")
        kwargs["headers"] = self.merge_headers(headers)

        # merge base and request kwargs
        kwargs = {**self.kwargs, **kwargs}

        # get request method
        request = getattr(self._session, method)

        try:
            # make request
            async with request(url=url, **kwargs) as response:
                # handle engine error message
                if response.status == 422:
                    # raise for api error
                    self.raise_for_api_error(await response.json(encoding="utf-8"))

                # handle other error messages
                response.raise_for_status()

                # decode application/json
                if content_type == "application/json":
                    json: dict[str, Any] = await response.json(encoding="utf-8")
                    return json

                # decode text/csv
                if content_type == "text/csv":
                    content: bytes = await response.read()
                    return BytesIO(content)

                # decode text/html
                if content_type == "text/html":
                    text: str = await response.text(encoding="utf-8")
                    return text

            raise NotImplementedError(f"Content-type '{content_type}' not implemented")

        finally:
            # handle session
            if not session:
                await self.close_async()
