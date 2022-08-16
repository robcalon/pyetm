from .curves import Curves
from .properties import Properties

class CustomCurves(Curves, Properties):
    
    def __init__(self):
        super().__init__(self)