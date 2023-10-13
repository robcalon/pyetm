"""Adapted from
https://github.com/pandas-dev/pandas/blob/main/pandas/compat/_optional.py"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

from pandas.util.version import Version

VERSIONS = {"aiohttp": "3.8.1", "xlsxwriter": "3.0"}


def get_version(module: ModuleType) -> str:
    """get version"""

    # get module version
    version = getattr(module, "__version__", None)

    # raise error
    if version is None:
        raise ImportError(f"Can't determine version for {module.__name__}")

    return version


def import_optional_dependency(name: str, min_version: str | None = None):
    """Import an optional dependency.

    By default, if a dependency is missing an ImportError with a nice
    message will be raised. If a dependency is present, but too old,
    we raise.

    Parameters
    ----------
    name : str
        The module name.
    min_version : str, default None
        Specify a minimum version that is different from the global pandas
        minimum version required.

    Returns
    -------
    maybe_module : Optional[ModuleType]
        The imported module, when found and the version is correct.
        None is returned when the package is not found and `errors`
        is False, or when the package's version is too old and `errors`
        is ``'warn'``.
    """
    try:
        module = importlib.import_module(name)

    except ImportError as exc:
        msg = (
            f"Missing optional dependency '{name}'."
            f"Use pip or conda to install {name}."
        )

        raise ImportError(msg) from exc

    # handle submodules
    parent = name.split(".")[0]
    if parent != name:
        install_name = parent
        module_to_get = sys.modules[install_name]

    else:
        module_to_get = module

    minimum_version = min_version if min_version is not None else VERSIONS.get(parent)
    if minimum_version:
        version = get_version(module_to_get)

        if version and Version(version) < Version(minimum_version):
            msg = (
                f"pyETM requires version '{minimum_version}' or newer of '{parent}' "
                f"(version '{version}' currently installed)."
            )

            raise ImportError(msg)

    return module
