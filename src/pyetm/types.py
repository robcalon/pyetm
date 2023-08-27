"""module defined types"""
from __future__ import annotations
from typing import Literal

import pandas as pd

Carrier = Literal["electricity", "heat", "hydrogen", "methane"]
ContentType = Literal["application/json", "text/csv", "text/html"]
ErrorHandling = Literal["ignore", "warn", "raise"]
Method = Literal["delete", "get", "post", "put"]


# class ETMTyped:
#     """module defined object base class"""


# class HourlyElectricityPriceCurve(pd.Series, ETMTyped):
#     """hourly electricity price curve"""


# class HourlyElectricityCurves(pd.DataFrame, ETMTyped):
#     """hourly electricity curves"""


# class HourlyHydrogenCurves(pd.DataFrame, ETMTyped):
#     """hourly hydrogen curves"""


# class HourlyMethaneCurves(pd.DataFrame, ETMTyped):
#     """hourly methane curves"""


# class HourlyHeatCurves(pd.DataFrame, ETMTyped):
#     """hourly heat curves"""


# class HourlyHouseholdCurves(pd.DataFrame, ETMTyped):
#     """hourly household curves"""
