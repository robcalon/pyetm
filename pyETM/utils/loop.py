"""create a thread in which a dedicated loop can run as an alternative 
to nesting, as this causes issues. This implementation is adapted
from https://stackoverflow.com/a/69514930"""

import asyncio
import threading

from pyETM.logger import get_modulelogger

# get modulelogger
logger = get_modulelogger(__name__)

def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

# create thread in which a new loop can run
_LOOP = asyncio.new_event_loop()
_LOOP_THREAD = threading.Thread(target=_start_loop, 
        args=[_LOOP], daemon=True)
