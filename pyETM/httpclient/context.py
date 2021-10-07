import asyncio
import nest_asyncio

class Context:
    
    @property
    def _ipython(self):
        return self.__ipython
    
    @_ipython.setter
    def _ipython(self, boolean):
        
        # check type
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