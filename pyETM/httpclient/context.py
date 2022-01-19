import os
import sys
import asyncio
import nest_asyncio

class Context:
    
    @property
    def proxy(self):
        return self.__proxy
        
    @proxy.setter
    def proxy(self, proxy):
        
        if not isinstance(proxy, str):
            raise TypeError('proxy must be of type string')
        
        if proxy == 'auto':
        
            # check environment variables for proxy settings
            if os.environ.get('HTTPS_PROXY') != '':
                proxy = os.environ.get('HTTPS_PROXY')

            elif os.environ.get('HTTP_PROXY') != '':
                proxy = os.environ.get('HTTP_PROXY')
                
            else:
                proxy = None

        self.__proxy = proxy
    
    @property
    def _ipython(self):
        return self.__ipython
    
    @_ipython.setter
    def _ipython(self, boolean):
        
        if boolean == 'auto':
            boolean = 'ipykernel' in sys.modules
        
        if not isinstance(boolean, bool):
            raise TypeError('"ipython" must be of type boolean')
        
        self.__ipython = boolean
        self._apply_context()
            
    def _apply_context(self):
        """apply context specific settings"""
        
        if self._ipython is True:
            
            # nest asyncio
            nest_asyncio.apply()
            
        try:
            
            # check for available loop
            loop = asyncio.get_event_loop()
                
        except RuntimeError:
            
            # create new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)