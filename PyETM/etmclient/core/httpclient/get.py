import json
import asyncio
import aiohttp

class Get:
    
    async def _async_get(self, post, proxy, **kwargs):
        """asynchronous wrapper for clientsession's get function."""
        
        # construct url
        url = self._make_url(post)

        # get url response and transform response into text
        async with aiohttp.ClientSession() as session:    
            async with session.get(url, proxy=proxy, **kwargs) as response:
                
                # check response
                valid = await self._check_response(response)
                            
                # check validity
                if not valid is True:
                    raise ValueError('check failed without raising error.')
                    
                # decode response
                decoded = await response.text(encoding='utf-8')
  
        return decoded

    def get(self, post, **kwargs):
        """Run async_get definition without async statement.
        
        Parameters
        ----------
        post : str
            String of the subdomain that is requested.

        Returns
        -------
        response : str
            Returns the decoded async response.
            
        **kwargs are passed to aiohttp's get function."""
        
        # pass proxy
        proxy = self.proxy
        
        # evaluate coroutine
        process = self._async_get(post, proxy=proxy, **kwargs)
        response = asyncio.run(process)
        
        return response