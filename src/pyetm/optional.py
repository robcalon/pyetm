"""Optinal imports"""
from __future__ import annotations

__all__ = ["import_optional_dependency"]

from importlib import import_module
from importlib.metadata import metadata, distribution, PackageNotFoundError
from types import ModuleType
from typing import Iterable

import re
import itertools

from packaging.requirements import Requirement
from pyetm import __package__ as _PACKAGE_

def _get_optional_requirements(
    distribution_name: str,
    exclude_extras: str | Iterable[str] | None = None
) -> list[Requirement]:
    """collect optional requirements from package metadata"""

    # convert string to set
    if isinstance(exclude_extras, str):
        exclude_extras = {exclude_extras}

    # get distribution metadata
    meta = metadata(distribution_name=distribution_name)

    # get extras and requirements from metadata
    extras = meta.get_all('Provides-Extra', failobj=[])
    reqs = meta.get_all('Requires-Dist', failobj=[])

    # drop excluded extra
    if exclude_extras is not None:
        extras = set(extras).difference(exclude_extras)

    # subset all optional requirments
    patterns = [f"extra == ['\"]{extra}['\"]" for extra in extras]
    reqs = [req for req in reqs if [pat for pat in patterns if re.search(pat, req)]]

    return list(set(Requirement(req.split(';')[0].strip()) for req in reqs))

def _yield_reqs_to_install(
    req: Requirement,
    current_extra: str | None = None
) -> Iterable[Requirement]:
    """yield installation requirements"""
    # https://github.com/HansBug/hbutils/blob/main/hbutils/system/python/package.py#L171

    current_extra = current_extra if current_extra else ''

    if req.marker and not req.marker.evaluate({'extra': current_extra}):
        return

    try:
        version = distribution(req.name).version
    except PackageNotFoundError:  # req not installed
        yield req
    else:
        if req.specifier.contains(version):
            for child_req in (metadata(req.name).get_all('Requires-Dist') or []):
                child_req_obj = Requirement(child_req)

                need_check, ext = False, None
                for extra in req.extras:
                    if child_req_obj.marker and child_req_obj.marker.evaluate({'extra': extra}):
                        need_check = True
                        ext = extra
                        break

                if need_check:  # check for extra reqs
                    yield from _yield_reqs_to_install(child_req_obj, ext)

        else:  # main version not match
            yield req


def _check_req(req: Requirement) -> bool:
    """check if any requirements is not installed"""
    # https://github.com/HansBug/hbutils/blob/main/hbutils/system/python/package.py#L198
    return not bool(list(itertools.islice(_yield_reqs_to_install(req), 1)))

def import_optional_dependency(
    module_name: str,
    exclude_extras: str | Iterable[str] | None = None,
    dependency_name: str | None = None
) -> ModuleType:
    """Import optional dependency

    Parameters
    ----------
    module_name: str
        name of optional dependency
    exclude_extras : str or Iterable, default None
        exclude specific extras from optional dependencies.
    dependency_name: str, default None
        Optional dependency name when module name differs from import name.
        Underscores in module names are automatically replaced with
        hyphens in dependency name construction by default.

    Returns
    -------
    module: ModuleType
        module of requested optional dependency
    """
    # https://github.com/pandas-dev/pandas/blob/main/pandas/compat/_optional.py#L83

    # default import name
    if dependency_name is None:
        dependency_name = module_name.replace('_', '-')

    # get optional requirements
    requirements = _get_optional_requirements(
        _PACKAGE_, exclude_extras=exclude_extras
    )

    # get the relevant optional dependency
    main = dependency_name.split('.', 1)[0]
    req = {req.name: req for req in requirements}.get(main)

    # handle unknown optional dependencies
    if req is None:
        raise ImportError(f"Optional dependency '{main}' not included in pyproject.toml")

    # return module if present
    if _check_req(req=req):
        return import_module(module_name)

    # get distribution version if installed
    try:
        version = distribution(req.name).version

    # handle missing optional dependency
    except PackageNotFoundError as exc:

        extras = ','.join(list(req.extras))

        if extras:
            extras = f"[{extras}]"

        msg = (
            f"Missing optional dependency '{req.name}'. "
            f"Use pip or conda to install {req.name}{extras}{req.specifier}."
        )

        raise ImportError(msg) from exc

    # handle wrong version
    msg = (
        f"{_PACKAGE_} requires version '{req.name}{req.specifier}' "
        f"(version '{version}' currently installed)."
    )

    raise ImportError(msg)
