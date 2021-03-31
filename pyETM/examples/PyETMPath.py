"""Add non-installed 'package' to system path"""

import os
import sys

# get modulepath
path = os.path.abspath(os.path.join('..'))
modulepath, module = os.path.split(path)

# add modulepath
sys.path.insert(0, modulepath)