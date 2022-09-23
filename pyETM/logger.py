from __future__ import annotations

import copy
import shutil
import logging

from pathlib import Path

def find_dirpath(dirname:  str, dirpath: str):
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
            raise ModuleNotFoundError("Could not find '%s' in '%s'"
                %(dirname, dirpath))

        # strip basename from modified path
        mdirpath = mdirpath.parent
        recursions += 1

    return mdirpath

def _create_mainlogger(logdir):
    """create main logger"""

    global _mainlogger

    # make logdir
    logdir = Path(logdir).joinpath('logs')
    Path.mkdir(logdir, exist_ok=True)

    # get rootlogger
    logger = logging.getLogger(PACKAGENAME)
    logger.setLevel(logging.DEBUG)

    # create formatter
    fmt = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    datefmt = '%Y-%m-%d %H:%M'
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    # create file handler
    filepath = logdir.joinpath(PACKAGENAME + '.log')
    file_handler = logging.FileHandler(filepath, mode='w+')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # create stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    # add handlers
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    _mainlogger = logger

def get_modulelogger(name: str):
    """get instance of modulelogger"""

    # create non existing loggers
    if not name in _moduleloggers.keys():
        _moduleloggers[name] = logging.getLogger(name)

    return _moduleloggers[name]

def export_logfile(dst: str | None = None):
    """Export logfile to targetfolder, 
    defaults to current working directory."""

    # default location
    if dst is None:
        dst = Path.cwd()
   
    # export file
    shutil.copyfile(LOGDIR, dst)

# package globals
PACKAGENAME = 'pyETM'
PACKAGEPATH = find_dirpath(PACKAGENAME, __file__)

# logger globals
LOGDIR = f"logs/{PACKAGENAME}.log"
LOGDIR = PACKAGEPATH.joinpath(LOGDIR).as_posix()

# track loggers
_mainlogger = None
_moduleloggers = {}

# initiate mainlogger
_create_mainlogger(PACKAGEPATH)
