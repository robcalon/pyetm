import asyncio
import nest_asyncio

class Context:
    
    @property
    def _context(self):
        
        # get context
        if self.__context is None:
            self._get_context()
            
        return self.__context
    
    @_context.setter
    def _context(self, context):
        self.__context = context
        self._apply_context()
            
    def _get_context(self):
        """get context in which object is created"""
        
        self.__context == 'native'
        
    def _apply_context(self):
        """apply context specific settings"""
        
        # notebook settings
        if self._context == 'notebook':
            nest_asyncio.apply()
        
        try:
            # check for available loop
            asyncio.get_event_loop()
                
        except RuntimeError:
            # create new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)