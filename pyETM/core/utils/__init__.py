from .categorisation import Categorisation
from .regionalisation import Regionalisation
from .templates import Templates

class Utils(Categorisation, Regionalisation, Templates):
    
    def __init__(self):
        super().__init__()
