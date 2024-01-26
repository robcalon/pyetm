"""module defined types"""
from __future__ import annotations
from os import PathLike
from typing import Literal, Mapping, Union


### GENERAL ###
ErrorHandling = Literal[
    "ignore",
    "warn",
    "raise"
]
StrOrPath = Union[str, PathLike[str]]

### SESSION ###
ContentType = Literal[
    "application/json",
    "text/csv",
    "text/html"
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
Method = Literal[
    "delete",
    "get",
    "post",
    "put"
]
TokenScope = Literal[
    "openid",
    "public",
    "scenarios:read",
    "scenarios:write",
    "scenarios:delete"
]

### SCENARIO ###
Carrier = Literal[
    "electricity",
    "heat",
    "hydrogen",
    "methane"
]

### PROFILES ###
ProfileType = Literal[
    'irradiance',
    'temperature',
    'windspeed',
    'solar_pv',
    'wind_offshore',
    'wind_onshore'
]
WeatherProfileMapping = Mapping[ProfileType, StrOrPath]
RegionMapping = Mapping[ProfileType, Mapping[str, str]]


### UTILITIES ###
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
