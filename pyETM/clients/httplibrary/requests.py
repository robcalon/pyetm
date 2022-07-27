import os
import io
import re
import logging
import requests

logger = logging.getLogger(__name__)


class UnprossesableEntityError(Exception):
    pass


class RequestsCore:
    
    @property
    def beta_engine(self):
        return self.__beta_engine
        
    @beta_engine.setter
    def beta_engine(self, boolean):

        # check instance
        if not isinstance(boolean, bool):
            raise TypeError('beta_engine must be a boolean')
            
        # set boolean
        self.__beta_engine = boolean
        
        # reset session
        self._reset_session()
    
    @property
    def proxies(self):
        return self.__proxies
    
    @proxies.setter
    def proxies(self, proxies):
        
        # get environment vars
        if proxies == "auto":

            # get proxies from environment
            http = os.environ.get("HTTP_PROXY")
            https = os.environ.get("HTTPS_PROXY")

            # revert to http
            if https == "":
                https = http

            # create proxies dict
            proxies = {'http': http, 'https': https}
        
        # set proxies
        self.__proxies = proxies
    
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
    
    def __handle_response(self, response, decoder=None):
        """handle API response"""
                
        # check response
        if not response.ok:
            
            # get debug message
            if response.status_code == 422:
                
                try:
                    # decode error message(s)
                    errors = response.json().get("errors")
                    
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
            response.raise_for_status()
                               
        if decoder == "json":
            return response.json()
        
        if decoder == "text":
            return response.text
        
        if decoder == "bytes":
            return io.BytesIO(response.content)
        
        return response        
        
    def post(self, url, decoder="json", **kwargs):
        """make post request"""
        
        # make target url
        url = self.__make_url(url)
        
        # post request
        with requests.Session() as session:
            with session.post(url=url, proxies=self.proxies, **kwargs) as resp:
                return self.__handle_response(resp, decoder=decoder)
                    
    def put(self, url, decoder="json", **kwargs):
        """make put request"""
        
        # make target url
        url = self.__make_url(url)
        
        # put request
        with requests.Session() as session:
            with session.put(url=url, proxies=self.proxies, **kwargs) as resp:
                return self.__handle_response(resp, decoder=decoder)

    def get(self, url, decoder="json", **kwargs):
        """make get request"""
        
        # make target url
        url = self.__make_url(url)

        # get request
        with requests.Session() as session:
            with session.get(url, proxies=self.proxies, **kwargs) as resp:
                return self.__handle_response(resp, decoder=decoder)
            
    def delete(self, url, decoder="text", **kwargs):
        """make delete request"""
        
        # make target url
        url = self.__make_url(url)

        # delete request
        with requests.Session() as session:
            with session.delete(url, proxies=self.proxies, **kwargs) as resp:
                return self.__handle_response(resp, decoder=decoder)

    def _get_session_id(self, scenario_id, **kwargs):
        """get a session_id for a pro-environment scenario"""    
        # get address
        host = "https://pro.energytransitionmodel.com"
        url = f"{host}/saved_scenarios/{scenario_id}/load"

        # get request
        with requests.Session() as session:
            with session.get(url=url, proxies=self.proxies, **kwargs) as resp:

                # get content
                content = resp.text
            
        # get session id
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        return session_id.group(1)            

    def upload_series(self, url, series, name=None, **kwargs):
        """upload series object"""
        
        # set key as name
        if name is None:
            name = "not specified"

        # convert series to string
        data = series.to_string(index=False)
        form = {"file": (name, data)}
        
        return self.put(url, files=form, **kwargs)
