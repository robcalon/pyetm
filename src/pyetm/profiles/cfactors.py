"""Helper to assign capacity factor names"""
from __future__ import annotations

import pandas as pd
from pyetm.utils.profiles import validate_profile


def validate_capacity_factors(
    wind_offshore: pd.Series[float],
    wind_onshore: pd.Series[float],
    wind_coastal: pd.Series[float],
    solar_pv: pd.Series[float],
    solar_thermal: pd.Series[float],
) -> pd.DataFrame:
    """validate profiles and assign correct keys"""

    profiles = [
        validate_profile(wind_offshore, name="weather/wind_offshore_baseline"),
        validate_profile(wind_onshore, name="weather/wind_inland_baseline"),
        validate_profile(wind_coastal, name="weather/wind_coastal_baseline"),
        validate_profile(solar_pv, name="weather/solar_pv_profile_1"),
        validate_profile(solar_thermal, name="weather/solar_thermal"),
    ]

    return pd.concat(profiles, axis=1).sort_index(axis=1)
