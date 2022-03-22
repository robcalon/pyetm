from .context import Context
from .delete import Delete
from .get import Get
from .post import Post
from .put import Put
from .response import Response


class HTTPClient(Context, Delete, Get, Post, Put, Response):
    
    def __init__(self):
        super().__init__()
