import json
import numpy
import pandas
import dateutil

from .interconnector.Interconnector import Interconnector


class Interconnectors:
            
    @property
    def interconnector_prices(self):
        
        # get interconnector pricecurves
        if self._interconnector_prices is None:
            self.get_interconnector_prices()
        
        return self._interconnector_prices
    
    @interconnector_prices.setter
    def interconnector_prices(self, curves):
        
        if curves is None:
            self.delete_interconnector_prices()
        else:
            self.change_interconnector_prices(curves)
    
    @property
    def interconnector_1(self):
        return self._interconnector_1
    
    @property
    def interconnector_2(self):
        return self._interconnector_2
    
    @property
    def interconnector_3(self):
        return self._interconnector_3
    
    @property
    def interconnector_4(self):
        return self._interconnector_4
    
    @property
    def interconnector_5(self):
        return self._interconnector_5
    
    @property
    def interconnector_6(self):
        return self._interconnector_6
    
    def _reset_interconnectors(self):
        """reset all interconnector properties"""
                
        # for convinience sake
        self._custom_fetched = False
            
        # make six interconnectors
        interconnectors = {}
        for number in range(6):
            interconnector = self._make_interconnector(number+1)
            interconnectors.update(interconnector)
        
        # reset all interconnectors
        self._interconnector_1 = interconnectors['interconnector_1']['object']
        self._interconnector_2 = interconnectors['interconnector_2']['object']
        self._interconnector_3 = interconnectors['interconnector_3']['object']
        self._interconnector_4 = interconnectors['interconnector_4']['object']
        self._interconnector_5 = interconnectors['interconnector_5']['object']
        self._interconnector_6 = interconnectors['interconnector_6']['object']
        
        # set interconnectors
        self._interconnectors = interconnectors
        
    def _make_interconnector(self, number):
        
        # name interconnector
        name = f'interconnector_{number}'

        # set default properties
        properties = {
            'object' : Interconnector(number, self),
            'pricekey' : name + '_price',
            'date' : None,
            'price' : None
        }

        # nest connector in dict
        connector = {name : properties}
        
        return connector
    
    def get_interconnector_prices(self, **kwargs):
        """get interconnector pricecurves"""
        
        # raise if scenario id is None
        self._raise_scenario_id()
        
        # prepare post
        headers = {'Connection': 'close'}
        post = f'/scenarios/{self.scenario_id}/custom_curves/'
        
        # request response and convert to dict
        response = self.get(post, headers=headers, **kwargs)
        data = json.loads(response)
        
        # update interconnector properties        
        for package in data:
            self._update_interconnector(package)
        
        # make prices dateframe
        prices = self._make_interconnector_prices()
        
        # set prices
        self._interconnector_prices = prices
        
        # update fetched parameter
        self._custom_fetched = True
        
        return prices
                    
    def change_interconnector_prices(self, curves):
        """change interconnector pricecurves"""
        
        # check if curves is dataframe
        if not isinstance(curves, pandas.DataFrame):
            raise TypeError('curves must be a pandas.DataFrame')
        
        # iterate over columns and change curves
        for curve in curves.columns:
            
            # get interconnector
            name = curve.replace('_price', '')
            interconnector = self._interconnectors[name]['object']
            
            # change curve
            interconnector.change_price(curves[curve])
            
    def delete_interconnector_prices(self):
        """delete interconnector pricecurves"""
        
        # iterate over interconnectors and delete curves
        for name in self._interconnectors.keys():
            
            # get interconnector
            interconnector = self._interconnectors[name]['object']

            # delete curve
            interconnector.delete_price()
            
    def _update_interconnector(self, package):
                    
        # get corresponsing interconnector
        name = package['type'].replace('_price', '')
        interconnector = self._interconnectors[name]

        # set information
        interconnector['price'] = package['stats']
        interconnector['date'] = self._unpack_date(package)
            
    def _unpack_date(self, package):
        
        # get date
        datetime = package['date']

        # transform date
        datetime = dateutil.parser.isoparse(datetime)
        datetime = datetime.astimezone(dateutil.tz.tzlocal())
        datetime = datetime.strftime("%d/%m/%Y %H:%M:%S %Z")
        
        return datetime
    
    def _make_interconnector_prices(self):
        
        # make empty dataframe
        idx = ['length', 'mean', 'min', 'min_at', 'max', 'max_at']
        prices = pandas.DataFrame(index=idx)
        
        # make priceframe
        for interconnector in self._interconnectors.keys():
            
            # get connector information
            pricekey = self._interconnectors[interconnector]['pricekey']
            price = self._interconnectors[interconnector]['price']
            
            # add data to dataframe
            prices[pricekey] = pandas.Series(price, dtype='object')
            
        return prices