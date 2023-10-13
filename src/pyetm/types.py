"""module defined types"""
from __future__ import annotations
from typing import Literal


Carrier = Literal["electricity", "heat", "hydrogen", "methane"]

ContentType = Literal["application/json", "text/csv", "text/html"]

ErrorHandling = Literal["ignore", "warn", "raise"]

Method = Literal["delete", "get", "post", "put"]

TokenScope = Literal[
    "openid", "public", "scenarios:read", "scenarios:write", "scenarios:delete"
]

Endpoint = Literal[
    "scenarios",
    "scenario_id",
    "curves",
    "custom_curves",
    "inputs",
    "merit_configuration",
    "user",
    "transition_paths",
    "token",
    "saved_scenarios",
]

# copied from pandas._typing
InterpolateOptions = Literal[
    "linear",
    "time",
    "index",
    "values",
    "nearest",
    "zero",
    "slinear",
    "quadratic",
    "cubic",
    "barycentric",
    "polynomial",
    "krogh",
    "piecewise_polynomial",
    "spline",
    "pchip",
    "akima",
    "cubicspline",
    "from_derivatives",
]

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
