"""CSV-based IO-handler for weather tools results"""

from __future__ import annotations

__all__ = ["CSVHandler"]

import pandas as pd

from pyetm.types import Carrier
from .abc import FileHandler

class CSVHandler(FileHandler):
    """CSV based IO-handler"""

    EXTENSION = '.csv'

    def read(
        self,
        region: str,
        cyear: int,
        scenario: str | None = None,
        carrier: Carrier | None = None,
        **kwargs
    ) -> pd.DataFrame:
        """read data"""

        # make filepath
        filepath = self.make_filepath(
            region=region,
            cyear=cyear,
            scenario=scenario,
            carrier=carrier
        )

        # handle data parser
        parse_dates = True
        if 'parse_dates' in kwargs:
            parse_dates = kwargs.pop('parse_dates')

        return pd.read_csv(filepath, parse_dates=parse_dates, **kwargs)

    def write(
        self,
        frame: pd.DataFrame,
        region: str,
        cyear: int,
        scenario: str | None = None,
        carrier: Carrier | None = None,
        **kwargs
    ) -> None:
        """write data"""

        # make filepath
        filepath = self.make_filepath(
            region=region,
            cyear=cyear,
            scenario=scenario,
            carrier=carrier
        )

        frame.to_csv(filepath, **kwargs)
