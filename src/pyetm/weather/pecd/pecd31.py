"""PECD31 Readers"""

from __future__ import annotations

__all__ = ["PECD31CSVReader"]

from datetime import datetime
from pathlib import Path

import pandas as pd

from pyetm.logger import get_modulelogger
from pyetm.types import ProfileType, WeatherProfileMapping

from .abc import PECDFileReader

logger = get_modulelogger(__name__)


class PECD31CSVReader(PECDFileReader):
    """Local files based PECD Reader for PECD31"""

    @property
    def mapper(self) -> WeatherProfileMapping | None:
        """mapping of profile file names"""
        return self._mapper

    @mapper.setter
    def mapper(self, mapper: WeatherProfileMapping | None) -> None:

        if mapper is None:
            self._mapper = None
            return None

        # iterate over profile dir mapping
        for profile, dirpath in mapper.items():

            # make filepath
            dirpath = self.wdir.joinpath(dirpath)

            # check if location exists
            if not dirpath.exists():
                raise FileExistsError(
                    f"Directory for '{profile}' does not exist: {dirpath}"
                )

            # check if location is a directory
            if not dirpath.is_dir():
                raise FileExistsError(
                    f"Path to directory for '{profile}' is not a directory: {dirpath}"
                )

            # get files with pattern
            files = Path(dirpath).glob("*.csv")

            # check is csv files in directory
            if len(list(files)) == 0:
                raise FileNotFoundError(f"CSV files not found in {dirpath}")

            # check for duplicates
            cyears = []
            for file in files:

                #  get cyear from filename
                cyear = file.stem[-4:]

                # raise error
                if cyear in cyears:
                    logger.warning(
                        "Duplicate csv-file for profile '%s' "
                        "in cyear '%s' in directory: %s",
                        profile, cyear, dirpath
                    )

                # append
                cyears.append(file.stem[-4:])

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

        # use mapped dirpath
        if self.mapper is not None:
            dirpath = self.wdir.joinpath(self.mapper.get(profile, profile))

        # use mapped dirpath
        else:
            dirpath = self.wdir.joinpath(f"{profile}")

        # get files for cyear
        files = list(dirpath.glob(f"*{cyear}.csv"))

        # validate file found
        if len(files) == 0:
            raise FileNotFoundError(
                f"File for profile '{profile}' with cyear "
                f"'{cyear}' not found in directory: {dirpath}"
            )

        # validate unique file found
        if len(files) > 1:
            raise ValueError(
                "Duplicate csv-file for profile "
                f"'{profile}' with cyear '{cyear}' in directory: {dirpath}"
            )

        # map region
        region = self._map_region(profile=profile, region=region)

        # read file
        series = pd.read_csv(
            files[0], usecols=[region]
        ).dropna().squeeze(axis=1)

        # assign periodindex
        series.index = pd.period_range(
            start=datetime(cyear, 1, 1),
            periods=len(series),
            freq='H',
            name='Period'
        )

        return pd.Series(series, name=profile, dtype=float)
