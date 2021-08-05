import json
import numpy

class FlexibilityOrder:
    
    @property
    def flexibility_order(self):
        
        # get flexibility order
        if self._flexibility_order is None:
            self.get_flexibility_order()
            
        return self._flexibility_order
    
    @flexibility_order.setter
    def flexibility_order(self, order):
        self.change_flexibility_order(order)
        
    def get_flexibility_order(self, **kwargs):
        """get the flexibility order"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/flexibility_order'
        
        # request response and convert to dict
        resp = self.get(post, headers=headers, **kwargs)
        order = json.loads(resp)
        
        # drop nestedness of order
        order = order['order']
        
        # store updated order
        self._flexibility_order = order
        
        return order
    
    def change_flexibility_order(self, order, **kwargs):
        """change flexibility order
        
        parameters
        ----------
        order : list
            desired flexibiity order"""
        
        # raise without scenario id
        self._raise_scenario_id()
        
        # check flexbility order
        order = self._check_flexibility_order(order)
        
        # map order to correct scenario parameter
        data = {'flexibility_order': {'order': order}}
        
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}/flexibility_order'
        
        # evaluate post
        self.put(post, json=data, headers=headers, **kwargs)
        
        # reinitialize scenario
        self._reset_session()
        
    def _check_flexibility_order(self, order):
        """check if items in flexbility order are in ETM."""
        
        # convert np,array to list
        if isinstance(order, numpy.ndarray):
            order = order.tolist()
            
        # access dict for order
        if isinstance(order, dict):
            order = order['order']

        # check items in order
        for item in order:
            if item not in self.flexibility_order:
                raise ValueError(f'"{item}" is not permitted as ' + 
                                 'flexibility order item in ETM')
        
        return order
    