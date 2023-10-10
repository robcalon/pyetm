"""Base Session"""
from __future__ import annotations
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any, Literal, Mapping, overload
from urllib.parse import urljoin

import re

import pandas as pd

from pyetm.exceptions import UnprossesableEntityError
from pyetm.types import ContentType, Method
from pyetm.utils.general import mapping_to_str


class SessionABC(ABC):
    """Session abstract base class for properties and methods
    accessed by ETM Client object."""

    @property
    @abstractmethod
    def headers(self) -> dict[str, str]:
        """headers send with each request"""

    @headers.setter
    @abstractmethod
    def headers(self, headers: dict[str, str] | None):
        pass

    @abstractmethod
    def connect(self) -> Any:
        """open session connection"""

    @abstractmethod
    def close(self) -> None:
        """close session connection"""

    @abstractmethod
    def delete(
        self, url: str, headers: Mapping[str, str] | None = None
    ) -> dict[str, Any]:
        """delete request"""

    @abstractmethod
    @overload
    def get(
        self,
        url: str,
        content_type: Literal["application/json"],
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        pass

    @abstractmethod
    @overload
    def get(
        self,
        url: str,
        content_type: Literal["text/csv"],
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> BytesIO:
        pass

    @abstractmethod
    @overload
    def get(
        self,
        url: str,
        content_type: Literal["text/html"],
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        pass

    @abstractmethod
    def get(
        self,
        url: str,
        content_type: ContentType,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any] | BytesIO | str:
        """get request"""

    @abstractmethod
    def post(
        self,
        url: str,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """post request"""

    @abstractmethod
    def put(
        self,
        url: str,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """put request"""

    @abstractmethod
    def upload(
        self, url: str, series: pd.Series, filename: str | None = None
    ) -> dict[str, Any]:
        """upload series request"""

    @staticmethod
    def make_url(base: str, url: str | None, allow_fragments: bool = True):
        """join base url with relative path"""
        return urljoin(base, url, allow_fragments)


class SessionTemplate(SessionABC):
    """Predefined session template that is adopted
    by the default session objects in pyetm."""

    @property
    def context(self):
        """passed session environment parameters"""

        # check for attribute
        if not hasattr(self, "_context"):
            self.context = None

        return self._context

    @context.setter
    def context(self, env: dict[str, Any] | None):
        self._context = dict(env) if env else {}

    @property
    def kwargs(self):
        """passed session kwargs"""

        # check for attribute
        if not hasattr(self, "_kwargs"):
            self.kwargs = None

        return self._kwargs

    @kwargs.setter
    def kwargs(self, kwargs: dict[str, Any] | None):
        self._kwargs = dict(kwargs) if kwargs else {}

    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return f"{self}({mapping_to_str({**self.kwargs, **self.context})})"

    def __enter__(self):
        self.connect()

    def __exit__(self, *args, **kwargs):
        self.close()

    @property
    def headers(self):
        """headers that are passed in each request"""

        # use default headers
        if not hasattr(self, "_headers"):
            self.headers = None

        return self._headers

    @headers.setter
    def headers(self, headers: dict[str, str] | None) -> None:
        self._headers = dict(headers) if headers else {}

    def delete(
        self,
        url: str,
        headers: Mapping[str, str] | None = None,
    ):
        """delete request"""
        return self.request(
            method="delete",
            url=url,
            content_type="text/html",
            headers=headers,
        )

    def get(
        self,
        url: str,
        content_type: ContentType,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ):
        """get request"""
        return self.request(
            method="get",
            url=url,
            content_type=content_type,
            headers=headers,
            params=params,
        )

    def post(
        self,
        url: str,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ):
        """post request"""
        return self.request(
            method="post",
            url=url,
            content_type="application/json",
            json=json,
            headers=headers,
        )

    def put(
        self,
        url: str,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ):
        """put request"""
        return self.request(
            method="put",
            url=url,
            content_type="application/json",
            json=json,
            headers=headers,
        )

    @abstractmethod
    @overload
    def request(
        self,
        method: Method,
        url: str,
        content_type: Literal["application/json"],
        **kwargs,
    ) -> dict[str, Any]:
        pass

    @abstractmethod
    @overload
    def request(
        self,
        method: Method,
        url: str,
        content_type: Literal["text/csv"],
        **kwargs,
    ) -> BytesIO:
        pass

    @abstractmethod
    @overload
    def request(
        self,
        method: Method,
        url: str,
        content_type: Literal["text/html"],
        **kwargs,
    ) -> str:
        pass

    @abstractmethod
    def request(
        self, method: Method, url: str, content_type: ContentType, **kwargs
    ) -> Any | dict[str, Any] | BytesIO | str:
        """make request"""

    def merge_headers(self, headers: dict[str, str] | None):
        """merge headers"""

        # no additional headers
        if headers is None:
            return self.headers

        return {**headers, **self.headers}

    def raise_for_api_error(self, message: dict[str, str]):
        """format API returned error messages"""

        # newlist
        errs = []

        # iterate over messages
        for error in message["errors"]:
            # format share group errors
            if "group does not balance" in error:
                error = self.format_share_group_error(error)

            # append to list
            errs.append(error)

        # make final message
        base = "ETEngine returned the following error(s):"
        msg = """%s\n > {}""".format("\n > ".join(errs)) % base

        # format error messages(s)
        raise UnprossesableEntityError(msg)

    @staticmethod
    def format_share_group_error(error: str) -> str:
        """apply more readable format to share group
        errors messages"""

        # find share group
        pattern = re.compile('"[a-z_]*"')
        group: str = pattern.findall(error)[0]

        # find group total
        pattern = re.compile(r"\d*[.]\d*")
        group_sum = pattern.findall(error)[0]

        # reformat message
        group = group.replace('"', "'")
        group = f"Share_group {group} sums to {group_sum}"

        # find parameters in group
        pattern = re.compile("[a-z_]*=[0-9.]*")
        items: list[str] = pattern.findall(error)

        # reformat message
        items = [item.replace("=", "': ") for item in items]
        msg = "'" + ",\n    '".join(items)

        return f"""{group}\n   {{{msg}}}"""
