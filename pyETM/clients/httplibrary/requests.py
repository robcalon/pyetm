import os
import io
import re
import logging
import requests

logger = logging.getLogger(__name__)


class UnprossesableEntityError(Exception):
    pass


class RequestsCore:
    
    @classmethod
    def _create_session(cls, **kwargs):
        """create session to connect with API
        
        For now only support for following environment settings from 
        the requests module:
        
        proxies: dict (optional), default None
            Dictionary mapping protocol or protocol and 
            hostname to the URL of the proxy.
        stream: boolean (optional), default False 
            Whether to immediately download the response content.
        verify: boolean or string (optional), default True
            Either a boolean, in which case it controls whether we verify
            the server's TLS certificate, or a string, in which case it must 
            be a path to a CA bundle to use. When set to False, requests will 
            accept any TLS certificate presented by the server, and will ignore 
            hostname mismatches and/or expired certificates, which will make 
            your application vulnerable to man-in-the-middle (MitM) attacks. 
            Setting verify to False may be useful during local development or 
            testing.
        cert: string or tuple (optional) 
            If string; path to ssl client cert file (.pem). 
            If tuple; ('cert', 'key') pair."""

        # check for unsupported kwargs
        supported = ['proxies', 'stream', 'verify', 'cert']
        invalid = [k for k in kwargs.keys() if k not in supported]

        if invalid:
            # raise error
            raise KeyError(f"passed kwarg(s) '{invalid}' not (yet) supported")

        # create session
        cls._session_kwargs = kwargs
        cls._session = requests.Session()

    @property
    def beta_engine(self):
        """connects to beta-engine when False and to production-engine
        when True.""" 
        return self.__beta_engine
        
    @beta_engine.setter
    def beta_engine(self, boolean: bool):
        """set beta engine attribute"""
            
        # set boolean and reset session
        self.__beta_engine = bool(boolean)
        self._reset_session()
        
    @property
    def base_url(self):
        """"base url for carbon transition model"""
        
        # return beta engine url
        if self.beta_engine:
            return "https://beta-engine.energytransitionmodel.com/api/v3"
        
        # return production engine url
        return "https://engine.energytransitionmodel.com/api/v3"
        
    def __make_url(self, url):
        """join url with base url"""
        return self.base_url + url

    def __request(self, method, url, **kwargs):
        """make request to connected session."""

        retries = 5
        while retries:

            try:

                # merge session and passed kwargs
                kwargs = {**self._session_kwargs, **kwargs}
                return self._session.request(method, url, **kwargs)

            except requests.ConnectionError as error:
                retries -= 1

        raise error

    def __decode(self, resp, decoder):
        """decode the response to specified type"""

        # no decoding
        if decoder is None:
            return resp

        # json decoding
        if decoder == "json":
            return resp.json()
        
        # text decoding
        if decoder == "text":
            return resp.text
        
        # bytes decoding
        if decoder == "bytes":
            return io.BytesIO(resp.content)

    def _request(self, method, url, decoder=None, **kwargs):
        """request and handle API response"""

        # get session response
        resp = self.__request(method, url, **kwargs)

        # check response
        if not resp.ok:
            
            # get debug message
            if resp.status_code == 422:
                
                try:
                    
                    # decode error message(s)
                    msg = self.__decode(resp, "json")
                    errors = msg.get("errors")
                    
                except:
                    
                    # no message returned
                    errors = None
                    
                # trigger special raise
                if errors:
                    
                    # create error report
                    base = "ETEngine returned the following error message(s):"
                    msg = """%s\n > {}""".format("\n > ".join(errors)) %base

                    raise UnprossesableEntityError(msg)
                                                        
            # raise status error
            resp.raise_for_status()
                                       
        return self.__decode(resp, decoder)        

    def _get_session_id(self, scenario_id, **kwargs):
        """get a session_id for a pro-environment scenario"""    

        # make pro url
        host = "https://pro.energytransitionmodel.com"
        url = f"{host}/saved_scenarios/{scenario_id}/load"

        # extract content from url
        content = self._request("GET", url, decoder='text', **kwargs)
            
        # get session id from content
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        return session_id.group(1)  

    def delete(self, url: str, decoder: str = 'text', **kwargs):
        """delete request to api"""
        url = self.__make_url(url)
        return self._request("DELETE", url, decoder=decoder, **kwargs)

    def get(self, url: str, decoder: str = 'json', **kwargs):
        """get request to api"""
        url = self.__make_url(url)
        return self._request("GET", url, decoder=decoder, **kwargs)
            
    def post(self, url: str, decoder: str = 'json', **kwargs):
        """post request to api"""
        url = self.__make_url(url)
        return self._request("POST", url, decoder=decoder, **kwargs)

    def put(self, url: str, decoder: str = 'json', **kwargs):
        """put request to api"""
        url = self.__make_url(url)
        return self._request("PUT", url, decoder=decoder, **kwargs)

    def upload_series(self, url, series, name=None, **kwargs):
        """upload series object"""
        
        # set key as name
        if name is None:
            name = "not specified"

        # convert series to string
        data = series.to_string(index=False)
        form = {"file": (name, data)}
        
        return self.put(url, files=form, **kwargs)
