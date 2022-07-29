import os
import logging

basedir = os.path.dirname(__file__)
logdir = '%s' %basedir
os.makedirs(logdir, exist_ok=True)

# create pyETM module logger
modulelogger = logging.getLogger("pyETM")
modulelogger.setLevel(logging.DEBUG)

# create formatter
fmt = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
datefmt = '%Y-%m-%d %H:%M'
formatter = logging.Formatter(fmt, datefmt=datefmt)

# create file handler
file = '%s/pyETM.log' %logdir
file_handler = logging.FileHandler(file, mode='w+')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# # create stream handler
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
# stream_handler.setLevel(logging.WARNING)

# add handlers
modulelogger.addHandler(file_handler)
# modulelogger.addHandler(stream_handler)