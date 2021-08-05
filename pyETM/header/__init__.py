import json

from .area_code import AreaCode
from .created_at import CreatedAt
from .display_group import DisplayGroup
from .end_year import EndYear
from .ordering import Ordering
from .protected import Protected
from .scaling import Scaling
from .source import Source
from .start_year import StartYear
from .template import Template
from .title import Title
from .url import URL


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