from .categorization import Categorization, categorize_curves
from .interpolation import Interpolator, interpolate_clients
from .regionalization import Regionalization, regionalize_curves
from .templates import Templates

class Utils(Categorization, Regionalization, Templates):
    
    def __init__(self):
        super().__init__()