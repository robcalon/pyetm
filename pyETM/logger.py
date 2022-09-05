from __future__ import annotations

import os
import logging

from pathlib import Path

def find_dirpath(dirname:  str, dirpath: str):
    """reduce dirpath until dirname in path found"""

    # convert to Path
    dirpath = Path(dirpath)

    # set modified dirpath
    mdirpath = os.path.dirname(dirpath)
    recursions, max_recursions = 0, len(dirpath.parts)

    # iterate over dirpath until basename matched dirname
    while os.path.basename(mdirpath) != dirname:

        # limit number of recursions
        if recursions >= max_recursions:
            raise ModuleNotFoundError("Could not find '%s' in '%s'"
                %(dirname, dirpath))

        # strip basename from modified path
        mdirpath = os.path.dirname(mdirpath)
        recursions += 1

    return str(mdirpath)

def _create_mainlogger(logdir):
    """create main logger"""

    global _mainlogger

    # make logdir
    logdir = '%s/logs' %logdir
    os.makedirs(logdir, exist_ok=True)

    # get rootlogger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create formatter
    fmt = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    datefmt = '%Y-%m-%d %H:%M'
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    # create file handler
    filepath = '%s/%s.log' %(logdir, PACKAGENAME)
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

# globals
PACKAGENAME = 'pyETM'
PACKAGEPATH = find_dirpath(PACKAGENAME, __file__)

# track loggers
_mainlogger = None
_moduleloggers = {}

# initiate mainlogger
_create_mainlogger(PACKAGEPATH)