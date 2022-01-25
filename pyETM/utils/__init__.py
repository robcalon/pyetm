from .categorization import Categorization, categorize_curves
from .interpolation import Interpolator, interpolate_clients
from .templates import Templates

class Utils(Categorization, Templates):
    
    def __init__(self):
        super().__init__()