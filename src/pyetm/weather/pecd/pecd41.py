"""PECD31 Readers"""

from __future__ import annotations

__all__ = ["PECD41CSVReader"]

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.types import ProfileType, WeatherProfileMapping

from .abc import PECDFileReader

logger = get_modulelogger(__name__)


class PECD41CSVReader(PECDFileReader):
    """Local files based PECD 4.1 Reader"""

    @property
    def mapper(self) -> WeatherProfileMapping | None:
        """mapping of profile file names"""
        return self._mapper

    @mapper.setter
    def mapper(self, mapper: WeatherProfileMapping | None) -> None:

        if mapper is None:
            self._mapper = mapper
            return None

        for profile, filepath in mapper.items():

            # make filepath
            filepath = self.wdir.joinpath(filepath)

            # check if file exists
            if not filepath.exists():
                raise FileExistsError(
                    f"File for profile '{profile}' does not exist: {filepath}"
                )

            # check if file is file
            if not filepath.is_file():
                raise FileExistsError(
                    f"File for profile '{profile}' is not a file: {filepath}"
                )

            # check if file is csv file
            if not filepath.suffixes[0] == '.csv':
                raise FileExistsError(
                    f"File for profile '{profile}' is not a CSV-file: {filepath}."
                )

        # set mapper
        self._mapper = mapper
        return None

    def read_profile(
        self,
        profile: ProfileType,
        region: str,
        cyear: int,
    ) -> pd.Series[float]:
        """read profile"""

        # use mapped filepath
        if self.mapper is not None:
            filepath = self.wdir.joinpath(self.mapper.get(profile, profile))

        # use mapped filepath
        else:
            filepath = self.wdir.joinpath(f"{profile}.csv")

        # map region
        region = self._map_region(profile=profile, region=region)

        # handle skiprows
        skiprows = None
        if profile not in ['wind_offshore', 'wind_onshore']:
            skiprows = 52

        # read part of csv
        series: pd.Series[float] = pd.read_csv(
            filepath,
            skiprows=skiprows,
            usecols=['Date', str(region)],
            index_col='Date',
            parse_dates=True
        ).dropna().squeeze(axis=1)

        # ensure periodindex
        series.index = pd.PeriodIndex(series.index, freq='H', name='Period')

        # check if year can be subsetted
        if int(cyear) not in series.index.year:
            raise KeyError(f"No data in dataset for year: {cyear}")

        # subset year
        series = series.loc[series.index.year == int(cyear)]

        return pd.Series(series, name=profile, dtype=float)
