import json
import asyncio
import aiohttp

class Post:
    
    async def _async_post(self, post, proxy, **kwargs):
        """asynchronous wrapper for clientsession's post function."""

        # construct url
        url = self._make_url(post)

        # post json at url and tranform response into json
        async with aiohttp.ClientSession() as session:
            async with session.post(url, proxy=proxy, **kwargs) as response:

                # check response
                valid = await self._check_response(response)
                            
                # check validity
                if not valid is True:
                    raise ValueError('check failed without raising error.')
                
                # decode response
                decoded = await response.json(encoding='utf-8')
                    
        return decoded
    
    def post(self, post, **kwargs):
        """Run async_post definition without async statement.
        
        Parameters
        ----------
        post : str
            String of the subdomain that is requested.
            
        Returns
        -------
        response : dict
            Returns the decoded async response.
            
        **kwargs are passed to aiohttp's post function."""

        # pass proxy
        proxy = self.proxy
        
        # evaluate coroutine
        process = self._async_post(post, proxy=proxy, **kwargs)
        response = asyncio.run(process)
        
        return response