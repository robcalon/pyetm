import os
import io
import re
import sys
import logging
import asyncio
import aiohttp
import nest_asyncio

logger = logging.getLogger(__name__)


class UnprossesableEntityError(Exception):
    pass


class AIOHTTPCore:
    
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
    def proxy(self):
        return self.__proxy
    
    @proxy.setter
    def proxy(self, proxy):
        
        # detect proxy
        if proxy == "auto":
            proxy = os.environ.get("HTTP_PROXY", None)
        
        # empty proxy
        if proxy == "":
            proxy = None
        
        # set proxy
        self.__proxy = proxy
        
    @property
    def _ipython(self):
        return self.__ipython
    
    @_ipython.setter
    def _ipython(self, boolean):
        
        # detect ipython usage
        if boolean == "auto":
            boolean = "ipykernel" in sys.modules
        
        # typecheck boolean
        if not isinstance(boolean, bool):
            raise TypeError("'ipython' must be of type boolean")
        
        # set boolean
        self.__ipython = boolean
        
        # apply context
        self.__apply_context()
    
    def __apply_context(self):
        """apply context specific settings"""
        
        if self._ipython:
            
            # nest asyncio
            nest_asyncio.apply()
            
        try:
            
            # check for available loop
            loop = asyncio.get_event_loop()
                
        except RuntimeError:
            
            # create new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
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
                
    async def __handle_response(self, response, decoder=None):
        """handle API response"""
        
        # check response
        if not (response.status <= 400):
                                    
            # get debug message
            if response.status == 422:
                
                try:
                    # decode error message(s)
                    errors = await response.json()
                    errors = errors.get("errors")
                    
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
            return await response.json(encoding="utf-8")
        
        if decoder == "text":
            return await response.text(encoding="utf-8")
        
        if decoder == "bytes":
            return await io.BytesIO(response.read())

        return await response    
                            
    async def __post(self, url, decoder=None, **kwargs):
        """make post request"""
        
        # make target url
        url = self.__make_url(url)
        
        # post request
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, **kwargs) as resp:
                return await self.__handle_response(resp, decoder=decoder)
            
    def post(self, url, decoder="json", **kwargs):
        """make post request"""
        return asyncio.run(self.__post(url, decoder, proxy=self.proxy, 
                                       **kwargs))
    
    async def __put(self, url, decoder=None, **kwargs):
        """make put request"""
        
        # make target url
        url = self.__make_url(url)
        
        # put request
        async with aiohttp.ClientSession() as session:
            async with session.put(url=url, **kwargs) as resp:
                return await self.__handle_response(resp, decoder=decoder)

    def put(self, url, decoder="json", **kwargs):
        """make put reqiest"""
        return asyncio.run(self.__put(url, decoder, proxy=self.proxy, **kwargs))
            
    async def __get(self, url, decoder=None, **kwargs):
        """make get request"""
        
        # make target url
        url = self.__make_url(url)

        # get request
        async with aiohttp.ClientSession() as session:
            async with session.get(url, **kwargs) as resp:
                return await self.__handle_response(resp, decoder=decoder)
            
    def get(self, url,  decoder="json",**kwargs):
        """make get request"""
        return asyncio.run(self.__get(url, decoder, proxy=self.proxy, **kwargs))
            
    async def __delete(self, url, decoder=None, **kwargs):
        """make delete request"""
        
        # make target url
        url = self.__make_url(url)

        # delete request
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, **kwargs) as resp:
                return await self.__handle_response(resp, decoder=decoder)
            
    def delete(self, url, decoder="text", **kwargs):
        """make delete request"""
        return asyncio.run(self.__delete(decoder, proxy=self.proxy, **kwargs))
    
    async def __get_session_id(self, scenario_id, **kwargs):
        """get a session_id for a pro-environment scenario"""

        # get address
        host = "https://pro.energytransitionmodel.com"
        url = f"{host}/saved_scenarios/{scenario_id}/load"

        # get request
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, **kwargs) as resp:

                # await content
                content = await resp.text()

        # get session id
        pattern = '"api_session_id":([0-9]{6,7})'
        session_id = re.search(pattern, content)

        return session_id.group(1)

    def _get_session_id(self, scenario_id, **kwargs):
        """get a session_id for a pro-environment scenario"""
        return asyncio.run(self.__get_session_id(scenario_id, proxy=self.proxy, 
                                                 **kwargs))
    
    def upload_series(self, url, series, name=None, **kwargs):
        """upload series object"""
        
        # set key as name
        if name is None:
            name = "not specified"
        
        # convert values to string
        data = series.to_string(index=False)
        
        # insert data in form
        form = aiohttp.FormData()
        form.add_field("file", data, filename=name)

        return self.put(url, data=form, **kwargs)
