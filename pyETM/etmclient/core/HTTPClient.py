# import property classes
from .httpclient.context import Context
from .httpclient.delete import Delete
from .httpclient.get import Get
from .httpclient.post import Post
from .httpclient.put import Put
from .httpclient.response import Response


class HTTPClient(Context, Delete, Get, Post, Put, Response):
    
    def __init__(self):
        super().__init__()
        
    def __enter__(self):
        """Contextual enter"""
        return self
    
    def __exit__(self, *args, **kwargs):
        """Contextual exit"""
        return None
        
    def _make_url(self, post):
        """Merge base url and post"""   
        return self._base_url + post