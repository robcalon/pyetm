import numpy

class HeatNetworkOrder:
    
    @property
    def heat_network_order(self):
        
        # get heat network order
        if self._heat_network_order is None:
            self.get_heat_network_order()
            
        return self._heat_network_order
    
    @heat_network_order.setter
    def heat_network_order(self, order):
        self.change_heat_network_order(order)
        
    def get_heat_network_order(self, **kwargs):
        """get the heat network order"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare request
        headers = {'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}/heat_network_order'
        
        # request response and convert to dict
        resp = self.get(url, headers=headers, **kwargs)
        order = resp["order"]
                
        # set heat network order
        self._heat_network_order = order
        
        return order
    
    def change_flexibility_order(self, order, **kwargs):
        """change heat network order
        
        parameters
        ----------
        order : list
            desired heat_network order"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # check heat network order
        order = self._check_heat_network_order(order)
        
        # map order to correct scenario parameter
        data = {'heat_network_order': {'order': order}}
        
        # prepare request
        headers = {'Connection':'close'}
        url = f'/scenarios/{self.scenario_id}/heat_network_order'
        
        # evaluate request
        self.put(url, json=data, headers=headers, **kwargs)
        
        # reinitialize scenario
        self._reset_session()
        
    def _check_heat_network_order(self, order):
        """check if items in flexbility order are in ETM."""
        
        # convert np,array to list
        if isinstance(order, numpy.ndarray):
            order = order.tolist()
        
        # acces dict for order
        if isinstance(order, dict):
            order = order['order']

        # check items in order
        for item in order:
            if item not in self.heat_network_order:
                raise ValueError(f'"{item}" is not permitted as ' + 
                                 'heat network order item in ETM')
        
        return order