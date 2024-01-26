"""Scaler ABC"""

from __future__ import annotations
from abc import ABC, abstractmethod

import pandas as pd

class ProfileScaler(ABC):
    """Abstract Base Class for ProfileScaler"""

    @abstractmethod
    def scale(self, profile: pd.Series[float], scalar: float) -> pd.Series[float]:
        """scale profiles"""
