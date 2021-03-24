import json

# import property classes
from .header.area_code import AreaCode
from .header.created_at import CreatedAt
from .header.display_group import DisplayGroup
from .header.end_year import EndYear
from .header.ordering import Ordering
from .header.protected import Protected
from .header.scaling import Scaling
from .header.source import Source
from .header.start_year import StartYear
from .header.template import Template
from .header.title import Title
from .header.url import URL


class Header(AreaCode, CreatedAt, DisplayGroup, EndYear, Ordering, 
             Protected, Scaling, Source, StartYear, Template, Title, URL):
    
    def __init__(self):
        super().__init__()
    
    def _get_scenario_header(self, **kwargs):
        """get header of scenario"""
        
        # raise without scenario id
        self._raise_scenario_id()
                
        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}'
        
        # request response and convert to dict
        resp = self.get(post, headers=headers, **kwargs)
        header = json.loads(resp)
        
        # set to corresponding parameter
        self._scenario_header = header
        
        return header
    
    def _change_scenario_header(self, header, **kwargs):
        """change header of scenario"""

        # raise without scenario id
        self._raise_scenario_id()
        
        # set data
        data = {"scenario": header}

        # prepare post
        headers = {'Connection':'close'}
        post = f'/scenarios/{self.scenario_id}'
        
        # evaluate post
        self.put(post, json=data, **kwargs)
        
        # reinitialize scenario
        self._reset_session()