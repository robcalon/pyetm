"""logger module"""
from __future__ import annotations

# from datetime import datetime
from pathlib import Path

import os
import copy
import shutil
import logging

from pyetm import __package__ as _PACKAGE_

def _find_dirpath(dirname:  str, dirpath: str | os.PathLike) -> Path:
    """reduce dirpath until dirname in path found"""

    # convert to Path
    dirpath = Path(dirpath)

    # set modified dirpath
    mdirpath = copy.copy(dirpath)
    recursions, max_recursions = 0, len(dirpath.parts)

    # iterate over dirpath until basename matched dirname
    while mdirpath.stem != dirname:

        # limit number of recursions
        if recursions >= max_recursions:

            # make message
            msg = f"Could not find '{dirname} in '{dirpath}'"

            raise ModuleNotFoundError(msg)

        # strip basename from modified path
        mdirpath = mdirpath.parent
        recursions += 1

    return mdirpath

def _create_mainlogger(packagename: str, logdir: str | os.PathLike) -> logging.Logger:
    """create mainlogger"""

    # make logdir
    logdir = Path(logdir).joinpath('logs')
    Path.mkdir(logdir, exist_ok=True)

    # get rootlogger
    logger = logging.getLogger(packagename)
    logger.setLevel(logging.DEBUG)

    # create formatter
    fmt = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    datefmt = '%Y-%m-%d %H:%M'
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    # create file handler
    filepath = logdir.joinpath(packagename + '.log')
    file_handler = logging.FileHandler(filepath, mode='w+')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # # create stream handler
    stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.WARNING)

    # add handlers
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

def get_modulelogger(name: str) -> logging.Logger:
    """get instance of modulelogger"""

    # create non existing loggers
    if name not in _moduleloggers:
        _moduleloggers[name] = logging.getLogger(name)

    return _moduleloggers[name]

def export_logfile(dst: str | os.PathLike | None = None) -> None:
    """Export logfile to targetfolder,
    defaults to current working directory."""

    # default location
    if dst is None:
        dst = Path.cwd()

    # export file
    shutil.copyfile(_LOGDIR_, dst)

# def export_exception_logs(exc: Exception,
#     logger: logging.Logger | None = None) -> None:
#     """report error message and export logs"""
#     # TODO: Can this be part of exception handling?

#     # default logger
#     if logger is None:
#         logger = get_modulelogger(__name__)

#     # get current time
#     now = datetime.now()
#     now = now.strftime("%Y%m%d%H%M")

#     # make filepath
#     filepath = Path.cwd().joinpath(now + '.log')

#     # log exception as error
#     logger.error("Encountered error: exported logs to '%s'", filepath)
#     logger.debug("Traceback for encountered error:", exc_info=True)

#     # export logfile
#     export_logfile(filepath)

#     raise exc

# package globals
_PACKAGEPATH_ = _find_dirpath(_PACKAGE_, __file__)

# logger globals
_LOGDIR_ = f"logs/{_PACKAGE_}.log"
_LOGDIR_ = _PACKAGEPATH_.joinpath(_LOGDIR_).as_posix()

# initialize mainlogger
_MAINLOGGER = _create_mainlogger(_PACKAGE_, _PACKAGEPATH_)

# track moduleloggers
_moduleloggers = {}
