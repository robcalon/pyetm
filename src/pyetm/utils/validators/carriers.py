"""Validate Carrier"""
from __future__ import annotations

from typing import cast, get_args, Iterable

from pyetm.types import Carrier
from pyetm.utils.general import iterable_to_str

def validate_carriers(carriers: str | Iterable[str]) -> list[Carrier]:
    """validate if carrier is supported"""

    # handle single carrier
    if isinstance(carriers, str):
        carriers = [carriers]

    # subset errors
    errors = set(car for car in carriers if car not in get_args(Carrier))

    # raise error
    if errors:
        raise ValueError(f"Unsupported carriers: {iterable_to_str(errors)}")

    return [cast(Carrier, carrier) for carrier in carriers]
