from .context import Context
from .delete import Delete
from .get import Get
from .post import Post
from .put import Put
from .response import Response


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