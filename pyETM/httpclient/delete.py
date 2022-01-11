import json
import asyncio
import aiohttp

class Delete:
    
    async def _async_delete(self, post, proxy, **kwargs):
        """asynchronous wrapper for clientsession's delete function."""
        
        # construct url
        url = self._make_url(post)

        # delete request at url
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, proxy=proxy, **kwargs) as response:                
                
                # check response
                await self._check_response(response)
                    
        return response
    
    def delete(self, post, **kwargs):
        """Run async_delete definition without async statement.
        
        Parameters
        ----------
        post : str
            String of the subdomain that is requested.
            
        Returns
        -------
        response : dict
            Returns the decoded async response.
            
        **kwargs are passed to aiohttp's delete function."""
        
        # pass proxy
        proxy = self.proxy
        
        # evaluate coroutine
        process = self._async_delete(post, proxy=proxy, **kwargs)
        response = asyncio.run(process)
        
        return response
    