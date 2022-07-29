"""Adapted from
https://github.com/pandas-dev/pandas/blob/main/pandas/compat/_optional.py"""

from __future__ import annotations

import importlib
import sys
import warnings

from types import ModuleType
from pandas.util.version import Version

# A mapping from import name to package name (on PyPI) for packages where
# these two names are different.

VERSIONS = {"aiohttp": "3.8.1", "nest_asyncio": "1.5.3"}
INSTALL_MAPPING = {}

def get_version(module: ModuleType) -> str:
    
    # get module version
    version = getattr(module, "__version__", None)

    # raise error
    if version is None:

        # nest asyncio has no version
        if module == 'nest_asyncio':
            return ''

        raise ImportError(f"Can't determine version for {module.__name__}")

    return version

def import_optional_dependency(name: str, extra: str = "", 
    errors: str = "raise", min_version: str | None = None):
    """Import an optional dependency.
    
    By default, if a dependency is missing an ImportError with a nice
    message will be raised. If a dependency is present, but too old,
    we raise.
    
    Parameters
    ----------
    name : str
        The module name.
    extra : str
        Additional text to include in the ImportError message.
    errors : str {'raise', 'warn', 'ignore'}
        What to do when a dependency is not found or its version is too old.
        * raise : Raise an ImportError
        * warn : Only applicable when a module's version is to old.
          Warns that the version is too old and returns None
        * ignore: If the module is not installed, return None, otherwise,
          return the module, even if the version is too old.
          It's expected that users validate the version locally when
          using ``errors="ignore"`` (see. ``io/html.py``)
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

    assert errors in {"warn", "raise", "ignore"}

    package_name = INSTALL_MAPPING.get(name)
    install_name = package_name if package_name is not None else name

    msg = (
        f"Missing optional dependency '{install_name}'. {extra} "
        f"Use pip or conda to install {install_name}."
    )
    
    try:
        module = importlib.import_module(name)
    
    except ImportError:
    
        if errors == "raise":
            raise ImportError(msg)
    
        else:
            return None

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
    
            if errors == "warn":
                warnings.warn(msg, UserWarning)
                return None
    
            elif errors == "raise":
                raise ImportError(msg)

    return module
