from __future__ import annotations

import io
import re
import json
import logging
import requests

import pandas as pd

from pyETM.exceptions import UnprossesableEntityError
from pyETM.types import Decoder, Method

logger = logging.getLogger(__name__)


class RequestsCore:
    
    @property
    def beta_engine(self) -> bool:
        """connects to beta-engine when False and to production-engine
        when True.""" 
        return self.__beta_engine
        
    @beta_engine.setter
    def beta_engine(self, boolean: bool) -> None:
        """set beta engine attribute"""
            
        # set boolean and reset session
        self.__beta_engine = bool(boolean)
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

    def _request(self, method: Method, url: str, 
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
                            self.__error_report(resp)
                        
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
                        msg = "decoding method '%s' not implemented" %method
                        raise NotImplementedError(msg)

                    logger.debug("processed '%s' request with '%s' decoder", 
                            method, decoder)

                    return resp

            # except connectionerrors and retry
            except requests.exceptions.ConnectionError as error:
                retries -= 1
                
                # raise after retries
                if not retries:
                    raise error

    def __error_report(self, resp: requests.Response) -> None:
        """create error report when api returns error messages."""
        
        try:

            # attempt decode error message(s)
            msg = resp.json()
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

    def delete(self, url: str, decoder: Decoder = 'text', **kwargs):
        return self._request("delete", self.__make_url(url), decoder, **kwargs)

    def get(self, url: str, decoder: Decoder = 'json', **kwargs):
        return self._request("get", self.__make_url(url), decoder, **kwargs)
            
    def post(self, url: str, decoder: Decoder = 'json', **kwargs):
        return self._request("post", self.__make_url(url), decoder, **kwargs)

    def put(self, url: str, decoder: Decoder = 'json', **kwargs):
        return self._request("put", self.__make_url(url), decoder, **kwargs)

    def upload_series(self, url: str, series: pd.Series, 
            name: str | None = None, **kwargs):
        """upload series object"""
        
        # set key as name
        if name is None:
            name = "not specified"

        # convert series to string
        data = series.to_string(index=False)
        form = {"file": (name, data)}
        
        return self.put(url, files=form, **kwargs)

    def _get_session_id(self, scenario_id: int, **kwargs) -> int:
        """get a session_id for a pro-environment scenario"""    

        # make pro url
        host = "https://pro.energytransitionmodel.com"
        url = f"{host}/saved_scenarios/{scenario_id}/load"

        # extract content from url
        content = self._request("GET", url, decoder='text', **kwargs)
            
        # get session id from content
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        return int(session_id.group(1))
