import numpy
import pandas
import pyETM

def interpolate_clients(clients, cfill='linear', dfill='ffill'):
    """Interpolates the user values of the years between the
    passed clients. Uses a seperate method for continous and
    discrete user values. 
    
    Do note that the heat network order is not returned or
    interpolated by this function.
    
    Parameters
    ----------
    clients: list
        List of pyETM.client.Client objects that
        are used to interpolate the scenario.
    cfill : string, default 'linear'
        Method for filling continious user values
        between years of passed scenarios. Passed 
        method is directly passed to interpolation
        function of a DataFrame.
    dfill : string, default 'ffill'
        Method for filling discrete user values
        between years of passed scenarios. Passed
        method is directly passed to fillna 
        function of a DataFrame. 
        
    Returns
    -------
    uvalues : DataFrame
        Returns the (interpolated) user values of each scenario 
        that is inside the range of the years of the passed 
        clients. The DataFrame also includes the user values for
        the years of the passed clients."""
        
    return Interpolator(clients).interpolate(cfill, dfill)

class Interpolator:
    """Interpolator class object"""
    
    @property
    def clients(self):
        """client list"""
        return self.__clients
    
    @clients.setter
    def clients(self, clients):
        """client list setter"""
        
        # check if clients in listlike object
        if not isinstance(clients, list):
            try:
                clients = list(clients)

            except: 
                raise TypeError('clients must be of type list')
            
        # check client in list
        for client in clients:
            if not isinstance(client, [pyETM.Client, pyETM.AsyncClient]):
                raise TypeError('client must be of type pyETM.client.Client')
        
        # validate area codes and sort area codes
        self._validate_area_codes(clients)
        clients = self._sort_clients(clients)

        # check scenario parameters
        self._validate_scenario_parameters(clients)
        
        # set client list
        self.__clients = clients
        
    def __init__(self, clients):
        """"initialize interpolator
        
        Parameters
        ----------
        clients : list
            List of pyETM.client.Client objects that
            are used to interpolate the scenario. Clients 
            are by end year on initialization."""
        
        # set clients
        self.clients = clients        
    
    def interpolate(self, cfill='linear', dfill='ffill'):
        """Interpolates the user values of the years between the
        passed clients. Uses a seperate method for continous and
        discrete user values. 
        Do note that the heat network order is not returned or
        interpolated by this function.
        Parameters
        ----------
        cfill : string, default 'linear'
            Method for filling continious user values
            between years of passed scenarios. Passed 
            method is directly passed to interpolation
            function of a DataFrame.
        dfill : string, default 'ffill'
            Method for filling discrete user values
            between years of passed scenarios. Passed
            method is directly passed to fillna 
            function of a DataFrame. 
        Returns
        -------
        uvalues : DataFrame
            Returns the (interpolated) user values of each scenario 
            that is inside the range of the years of the passed 
            clients. The DataFrame also includes the user values for
            the years of the passed clients.
        make sure to check share groups after interpolation"""

        # get end years of client to make annual series
        years = [client.end_year for client in self.clients]
        columns = [x for x in range(min(years), max(years) + 1)]

        # get continous and discrete values for clients
        cvalues = [client._cvalues for client in self.clients]
        cvalues = pandas.concat(cvalues, axis=1, keys=years)
        
        # make cvalues dataframe and interpolate
        cvalues = pandas.DataFrame(data=cvalues, columns=columns)
        cvalues = cvalues.interpolate(method=cfill, axis=1)
        
        # get dvalues from passed clients
        dvalues = [client._dvalues for client in self.clients]
        dvalues = pandas.concat(dvalues, axis=1, keys=years)

        # merge cvalues and dvalues
        uvalues = pandas.concat([cvalues, dvalues])
        uvalues = uvalues.fillna(method=dfill, axis=1)

        # sort user values
        sparams = self.clients[0].scenario_parameters
        uvalues = uvalues.loc[sparams.index]
                
        return uvalues

    def _sort_clients(self, clients):
        """sort list of clients based on end year"""

        # get end years and sort clients based on end years
        years = [client.end_year for client in clients]
        clients = numpy.array(clients)[numpy.array(years).argsort()]

        # check for duplicate end years
        if len(years) != len(numpy.unique(years)):
            raise ValueError("clients passed with duplicate scenario end year")

        return list(clients)

    def _validate_area_codes(self, clients):
        """check if all area codes of the passed scenarios are the same"""

        # get all area codes of all clients
        codes = [client.area_code for client in clients]

        if len(numpy.unique(codes)) != 1:
            raise ValueError("different area codes in passed clients")
    
    def _validate_scenario_parameters(self, clients):
        """check if all set scenario paremeters are okay"""
        
        for client in clients:
            try:
                client._check_scenario_parameters()
                
            except ValueError:
                raise ValueError('errors in scenario parameters of ' +
                                 f'{client}, diagnose client with ' +
                                 'client._check_scenario_parameters()')