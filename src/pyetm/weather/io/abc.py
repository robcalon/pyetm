"""Abstract Base Class for Custom Curves Reader"""
from __future__ import annotations

__all__ = ["IOHandler", "FileHandler"]

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from pyetm.types import Carrier, StrOrPath


class IOHandler(ABC):
    """IO ABC for data sources"""

    @abstractmethod
    def read(
        self,
        region: str,
        cyear: int,
        scenario: str | None = None,
        carrier: Carrier | None = None,
    ) -> pd.DataFrame:
        """read data"""

    @abstractmethod
    def write(
        self,
        frame: pd.DataFrame,
        region: str,
        cyear: int,
        scenario: str | None = None,
        carrier: Carrier | None = None
    ) -> None:
        """write data"""

class FileHandler(IOHandler):
    """File based implementation"""

    EXTENSION = None
    SHEETNAME = None

    @property
    def wdir(self) -> Path:
        """target dir to store results"""
        return self._wdir

    @wdir.setter
    def wdir(self, wdir: StrOrPath | None) -> None:

        # default wdir
        if wdir is None:
            wdir = Path.cwd()

        # set wdir
        wdir = Path(wdir)

        # check if location exists
        if not wdir.exists():
            raise FileExistsError(f"Directory does not exist: {wdir}")

        # check if location is a directory
        if not wdir.is_dir():
            raise FileExistsError(f"Path to directory is not a directory: {wdir}")

        # set location
        self._wdir = wdir

    def __init__(self, wdir: StrOrPath | None = None):

        # set wdir
        self.wdir = wdir

    def make_filepath(
        self,
        region: str,
        cyear: int,
        scenario: str | None = None,
        carrier: Carrier | None = None
    ) -> Path:
        """make filepath"""

        # make filename equivelent to f-string:
        # f'{scenario}_{region}_{carrier}_{cyear}'
        params = scenario, region, carrier, cyear
        filename = '_'.join(map(str, filter(None, params)))

        # make filepath
        filepath: Path = self.wdir.joinpath(filename)

        # add extension
        if self.EXTENSION is not None:
            filepath = filepath.with_suffix(self.EXTENSION)

        return filepath

    @abstractmethod
    def read(
        self,
        region: str,
        cyear: int,
        scenario: str | None = None,
        carrier: Carrier | None = None,
    ) -> pd.DataFrame:
        """read data"""

    @abstractmethod
    def write(
        self,
        frame: pd.DataFrame,
        region: str,
        cyear: int,
        scenario: str | None = None,
        carrier: Carrier | None = None,
    ) -> None:
        """write data"""
