from .pricecurve import Pricecurve


class Interconnector(Pricecurve):

    def __init__(self, number, parent):
        
        # init self
        super().__init__()
        
        # set arguments
        self._name = f'interconnector_{number}'
        self._parent = parent
        
        # init hidden properties
        self._price = None
        self._price_upload_date = None
        
    @property
    def name(self):
        return self._name