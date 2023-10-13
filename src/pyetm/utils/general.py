"""general utilities"""
from __future__ import annotations

import re
from typing import Any, Iterable, Mapping


def bool_to_json(boolean: bool):
    """convert boolean to json compatible string"""
    return str(bool(boolean)).lower()


def iterable_to_str(iterable: Iterable[Any]):
    """transform list to string"""
    return ", ".join(map(str, iterable))


def mapping_to_str(mapping: Mapping[Any, Any]):
    """transform mapping to string"""
    return ", ".join(f"{key}={value}" for key, value in mapping.items())


def mapped_floats_to_str(mapping: Mapping[str, float | int], prec: int) -> str:
    """transform mapping to string with rounding of floats"""
    return ", ".join(f"{key}={value:.{prec}f}" for key, value in mapping.items())


def snake_case_name(cls: object) -> str:
    """convert camel case name to snake case name"""

    word = cls.__class__.__name__
    word = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", word)
    word = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", word)
    word = word.replace("-", "_")

    return word.lower()
