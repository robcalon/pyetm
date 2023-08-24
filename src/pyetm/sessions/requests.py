"""requests session object"""
from __future__ import annotations
from io import BytesIO
from typing import Any, overload, Literal

import pandas as pd
import requests

from pyetm.sessions.abc import SessionTemplate
from pyetm.types import ContentType, Method


class RequestsSession(SessionTemplate):
    """requests bases adaptation"""

    def __init__(
        self,
        proxies: dict | None = None,
        stream: bool = False,
        verify: bool | str = True,
        cert: str | tuple | None = None,
    ):
        """session object for pyETM clients

        Parameters
        ----------
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
            If tuple; ('cert', 'key') pair."""

        # set environment kwargs for method requests
        self.kwargs = {
            "proxies": proxies,
            "stream": stream,
            "verify": verify,
            "cert": cert,
        }

        # set session
        self._session = requests.Session()

    def connect(self):
        """connect session"""

    def close(self):
        """close session"""

    def upload(
        self,
        url: str,
        series: pd.Series,
        filename: str | None = None,
    ):
        """upload series"""

        # set key as name
        if filename is None:
            filename = "filename not specified"

        # convert series to string
        data = series.to_string(index=False)
        form = {"file": (filename, data)}

        return self.request(
            method="put", url=url, content_type="application/json", files=form
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
        url: str | None,
        content_type: ContentType,
        **kwargs,
    ):
        """make request to api session"""

        # merge base and request headers
        headers = kwargs.get("headers")
        kwargs["headers"] = self.merge_headers(headers)

        # merge base and request kwargs
        kwargs = {**self.kwargs, **kwargs}

        # get request method
        request = getattr(self._session, method)

        # make request
        with request(url=url, **kwargs) as response:
            # handle engine error message
            if response.status_code == 422:
                return self.raise_for_api_error(response.json())

            # handle other error messages
            response.raise_for_status()

            # decode application/json
            if content_type == "application/json":
                json: dict[str, Any] = response.json()
                return json

            # decode text/csv
            if content_type == "text/csv":
                content: bytes = response.content
                return BytesIO(content)

            # decode text/html
            if content_type == "text/html":
                text: str = response.text
                return text

        raise NotImplementedError(f"Content-type '{content_type}' not implemented")
