"""Parquet-based IO-handler for weather tools results"""

from __future__ import annotations

__all__ = ["ParquetHandler"]

import pandas as pd

from pyetm.types import Carrier
from .abc import FileHandler

class ParquetHandler(FileHandler):
    """Parquet based IO-handler"""

    EXTENSION = '.parquet'

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

        return pd.read_parquet(filepath, **kwargs)

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

        # write parquet
        frame.to_parquet(filepath, **kwargs)
