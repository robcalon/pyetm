from .categorization import Categorization
from .regionalization import Regionalization
from .templates import Templates

class Utils(Categorization, Regionalization, Templates):
    
    def __init__(self):
        super().__init__()
