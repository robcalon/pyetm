import pandas

class CreatedAt:
    
    @property
    def created_at(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        # get created at
        datetime = self._scenario_header['created_at']
        datetime = pandas.to_datetime(datetime, utc=True)
        
        return datetime