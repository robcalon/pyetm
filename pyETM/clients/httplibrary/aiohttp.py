from __future__ import annotations

import io
import re
import json
import logging
import asyncio

import pandas as pd

from typing import TYPE_CHECKING

from pyETM.types import Decoder, Method
from pyETM.exceptions import UnprossesableEntityError
from pyETM.optional import import_optional_dependency

if TYPE_CHECKING:
    import aiohttp

logger = logging.getLogger(__name__)

class AIOHTTPCore:
    
    @property
    def beta_engine(self) -> bool:
        return self.__beta_engine
        
    @beta_engine.setter
    def beta_engine(self, boolean: bool) -> None:

        # check instance
        if not isinstance(boolean, bool):
            raise TypeError('beta_engine must be a boolean')
            
        # set boolean
        self.__beta_engine = boolean
        
        # reset session
        self._reset_session()

    @property
    def base_url(self) -> str:
        """"base url for carbon transition model"""
        
        # return beta engine url
        if self.beta_engine:
            return "https://beta-engine.energytransitionmodel.com/api/v3"
        
        # return production engine url
        return "https://engine.energytransitionmodel.com/api/v3"

    def __make_url(self, url: str) -> str:
        """join url with base url"""
        return self.base_url + url

    async def _start_session(self):
        """start up session"""
        
        if TYPE_CHECKING:
            # import aiohttp
            import aiohttp

        else:
            # optional module import
            aiohttp = import_optional_dependency('aiohttp')

        self._session = aiohttp.ClientSession(**self._session_env)

    async def _close_session(self):
        """clean up session"""

        # close and remove session
        await self._session.close()
        self._session = None        
        
    async def _async_request(self, method: Method, url: str, 
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
                    await self._start_session()
                    logger.debug('single use session created')

                # make method request
                request = getattr(self._session, method)
                async with request(url, **kwargs) as resp:

                    # check response
                    if not (resp.status <= 400):
                                                
                        # report error messages
                        if resp.status == 422:
                            self.__error_report(resp)
                        
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

                    logger.debug("processed '%s' request with '%s' decoder", 
                            method, decoder)

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
                    await self._close_session()
                    logger.debug('single use session destroyed')

    async def __error_report(self, resp: aiohttp.ClientResponse) -> None:
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
                                                            
    def _request(self, method: Method, url: str, 
            decoder: Decoder = "bytes", **kwargs):
        """request and handle api response"""

        # specify coroutine and get future
        coro = self._async_request(method, url, decoder, **kwargs)
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)

        return future.result()
            
    def delete(self, url: str, decoder: Decoder = "text", **kwargs):
        """delete request to api"""
        return self._request("delete", self.__make_url(url), decoder, **kwargs)

    def get(self, url: str, decoder: Decoder = "json", **kwargs):
        """make get request"""
        return self._request("get", self.__make_url(url), decoder, **kwargs)

    def post(self, url: str, decoder: Decoder = "json", **kwargs):
        """make post request"""
        return self._request("post", self.__make_url(url), decoder, **kwargs)

    def put(self, url: str, decoder: Decoder = "json", **kwargs):
        """make put reqiest"""
        return self._request("put", self.__make_url(url), decoder, **kwargs)
                
    def upload_series(self, url: str, series: pd.Series, 
            name: str | None = None, **kwargs):
        """upload series object"""
        
        if TYPE_CHECKING:
            # import aiohttp
            import aiohttp

        else:
            # optional module import
            aiohttp = import_optional_dependency('aiohttp')

        # set key as name
        if name is None:
            name = "not specified"
        
        # convert values to string
        data = series.to_string(index=False)
        
        # insert data in form
        form = aiohttp.FormData()
        form.add_field("file", data, filename=name)

        return self.put(url, data=form, **kwargs)

    def _get_session_id(self, scenario_id: int, **kwargs) -> int:

        # make pro url
        host = "https://pro.energytransitionmodel.com"
        url = f"{host}/saved_scenarios/{scenario_id}/load"

        # extract content from url
        content = self._request("get", url, decoder='text', **kwargs)
            
        # get session id from content
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        return int(session_id.group(1))
