from __future__ import annotations

import io
import json
import requests

import pandas as pd
from urllib.parse import urljoin

from typing import Literal
from pyETM.exceptions import UnprossesableEntityError, format_error_messages

Decoder = Literal['bytes', 'BytesIO', 'json', 'text']
Method = Literal['delete', 'get', 'post', 'put']


class RequestsSession:

    @property
    def base_url(self) -> str | None:
        return self.__base_url

    @base_url.setter
    def base_url(self, base_url: str | None) -> None:

        if base_url is None:
            self.__base_url = base_url

        if base_url is not None:
            self.__base_url = str(base_url)

    def __init__(self, base_url: str | None = None, 
        proxies: dict | None = None, stream: bool = False, 
        verify: bool | str = True, cert: str | tuple | None = None):
        """session object for pyETM clients
                
        Parameters
        ----------
        base_url: str, default None
            Base url to which the session connects, all request urls
            will be merged with the base url to create a destination. 
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

        # set hidden values
        self.base_url = base_url

        # set environment kwargs for method requests
        self._request_env = {
            "proxies": proxies, "stream": stream,
            "verify": verify, "cert": cert}
    
        # set session
        self._session = requests.Session()

    def __repr__(self):
        """reproduction string"""

        # object environment
        env = ", ".join(f"{k}={str(v)}" for k, v in 
            self._request_env.items())

        return "RequestsSession(%s)" %env

    def __str__(self):
        """stringname"""
        return 'RequestsSession'

    def __enter__(self) -> RequestsSession:
        """enter context manager"""

        # connect session
        self.connect()

        return self

    def __exit__(self, *args, **kwargs) -> None:
        """exit context manager"""

        # close session
        self.close()

    def make_url(self, url: str | None = None):
        """join url with base url"""
        return urljoin(self.base_url, url)

    def connect(self):
        """connect session"""
        pass

    def close(self):
        """close session"""
        pass

    def request(self, method: Method, url: str, 
            decoder: Decoder = 'bytes', **kwargs):
        """make request to api session"""

        retries = 5
        while retries:

            try:

                # merge kwargs with session envioronment kwargs
                kwargs = {**self._request_env, **kwargs}

                # make method request
                request = getattr(self._session, method)
                with request(url, **kwargs) as resp:

                    # check response
                    if not resp.ok:
                        
                        # get debug message
                        if resp.status_code == 422:
                            self._error_report(resp)
                        
                        # raise for status
                        resp.raise_for_status()

                    # bytes decoding
                    if decoder == "bytes":
                        resp = resp.content

                    # bytes as BytesIO
                    elif decoder == "BytesIO":
                        byts = resp.content
                        resp = io.BytesIO(byts)

                    # json decoding
                    elif decoder == "json":
                        resp = resp.json()
                    
                    # text decoding
                    elif decoder == "text":
                        resp = resp.text

                    else:
                        msg = "decoding method '%s' not implemented" %decoder
                        raise NotImplementedError(msg)

                    return resp

            # except connectionerrors and retry
            except requests.exceptions.ConnectionError as error:
                retries -= 1

                # raise after retries
                if not retries:
                    raise error
                
    def _error_report(self, resp: requests.Response) -> None:
        """create error report when api returns error messages."""
        
        try:

            # attempt decode error message(s)
            msg = resp.json()
            errors = msg.get("errors")

        except json.decoder.JSONDecodeError:

            # no message returned
            errors = None

        if errors:

            # format error message(s)
            msg = format_error_messages(errors)
            raise UnprossesableEntityError(msg)

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
        
        # default to empty series
        if series is None:
            series = pd.Series()

        # set key as name
        if name is None:
            name = "not specified"

        # convert series to string
        data = series.to_string(index=False)
        form = {"file": (name, data)}
        
        return self.put(url=url, files=form, **kwargs)
