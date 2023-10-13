"""init sessions module"""
from .aiohttp import AIOHTTPSession
from .requests import RequestsSession

__all__ = ["AIOHTTPSession", "RequestsSession"]
