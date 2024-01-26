"""Sigmoid-based ProfileScaler"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .abc import ProfileScaler

class SigmoidScaler(ProfileScaler):
    """Sigmoid based profile scaler"""

    def scale(self, profile: pd.Series[float], scalar: float) -> pd.Series:
        """scale profile by applying sigmoid"""

        # check for values within range 0, 1

        # don't scale profile
        if (scalar is None) or (scalar == 1):
            return profile

        # get derivative, normalize and scale with volume
        factor = profile.apply(self._scaler)
        factor = factor / factor.sum() * (scalar - 1) * profile.sum()

        # recheck if values within range or warn user.

        return profile.add(factor)

    def _sigmoid(self, factor: float) -> float:
        """parameterised sigmoid function for normalized profiles"""
        return -7.9921 + (9.2005 / (1 + np.exp(-1.7363 * factor + -1.8928)))

    def _scaler(self, factor: float) -> float:
        """scale by derivative of sigmoid"""
        return self._sigmoid(factor) * (1 - self._sigmoid(factor))
