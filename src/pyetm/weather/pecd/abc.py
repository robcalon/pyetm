"""Readers for the Pan-European Climate Database"""
from __future__ import annotations

__all__ = [
    "PECDReader",
    "PECDFileReader",
]

from abc import ABC, abstractmethod
from pathlib import Path
from typing import get_args

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.types import ProfileType, StrOrPath, WeatherProfileMapping, RegionMapping

logger = get_modulelogger(__name__)

class PECDReader(ABC):
    """Abstract Base Class for PECD readers."""

    @abstractmethod
    def read_irradiance(self, region: str, cyear: int) -> pd.Series[float]:
        """read irradiance profile"""

    @abstractmethod
    def read_temperature(self, region: str, cyear: int) -> pd.Series[float]:
        """read temperature profile"""

    @abstractmethod
    def read_windspeed(self, region: str, cyear: int) -> pd.Series[float]:
        """read windspeed profile"""

    @abstractmethod
    def read_solar_pv(self, region: str, cyear: int) -> pd.Series[float]:
        """read solar pv profile"""

    @abstractmethod
    def read_wind_offshore(self, region: str, cyear: int) -> pd.Series[float]:
        """read offshore wind profile"""

    @abstractmethod
    def read_wind_onshore(self, region: str, cyear: int) -> pd.Series[float]:
        """read offshore wind profile"""

class PECDFileReader(PECDReader):
    """Base properties for PECD File Handling"""

    @property
    def regionmapper(self) -> RegionMapping | None:
        """mapping of region in profile to other region in profile."""
        return self._regionmapper

    @regionmapper.setter
    def regionmapper(self, regionmapper: RegionMapping | None) -> None:

        # accept argument
        if regionmapper is None:
            self._regionmapper = regionmapper
            return None

        # ensure correct keys
        for key in regionmapper.keys():
            if not key in get_args(ProfileType):
                raise ValueError(f"Unknown profile in regionmapper: '{key}'")

        # remove _OFF suffix from mappers
        _regionmapper = {}
        for profile, mapper in regionmapper.items():
            _mapper = {}
            for key, value in mapper.items():
                _mapper[key.removesuffix("_OFF")] = value.removesuffix("_OFF")
            _regionmapper[profile] = _mapper

        self._regionmapper = _regionmapper

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

    @property
    @abstractmethod
    def mapper(self) -> WeatherProfileMapping | None:
        """mapper"""

    @mapper.setter
    @abstractmethod
    def mapper(self, mapper: WeatherProfileMapping | None) -> None:
        pass

    def __init__(
        self,
        wdir: StrOrPath | None = None,
        mapper: WeatherProfileMapping | None = None,
        regionmapper: RegionMapping | None = None
    ) -> None:

        # set wdir
        self.wdir = wdir
        self.mapper = mapper
        self.regionmapper = regionmapper

    def _map_region(self, profile: ProfileType, region: str) -> str:
        """map region based on region mapper"""

        # handle region mapper for mapped regions
        if self.regionmapper is not None:
            if profile in self.regionmapper.keys():
                if region in self.regionmapper[profile].keys():
                    region = self.regionmapper[profile][region]

        # handle offshore regions
        if profile == "wind_offshore":
            region = f"{region}_OFF"

        return region

    @abstractmethod
    def read_profile(
        self,
        profile: ProfileType,
        region: str,
        cyear: int
    ) -> pd.Series[float]:
        """read profile"""

    def read_irradiance(self, region: str, cyear: int) -> pd.Series[float]:
        """read irradiance profile"""
        return self.read_profile("irradiance", region=region, cyear=cyear)

    def read_temperature(self, region: str, cyear: int) -> pd.Series[float]:
        """read temperature profile"""
        return self.read_profile("temperature", region=region, cyear=cyear)

    def read_windspeed(self, region: str, cyear: int) -> pd.Series[float]:
        """read windspeed profile"""
        return self.read_profile("windspeed", region=region, cyear=cyear)

    def read_solar_pv(self, region: str, cyear: int) -> pd.Series[float]:
        """read solar pv profile"""
        return self.read_profile("solar_pv", region=region, cyear=cyear)

    def read_wind_offshore(self, region: str, cyear: int) -> pd.Series[float]:
        """read offshore wind profile"""
        return self.read_profile("wind_offshore", region=region, cyear=cyear)

    def read_wind_onshore(self, region: str, cyear: int) -> pd.Series[float]:
        """read onshore wind profile"""
        return self.read_profile("wind_onshore", region=region, cyear=cyear)
