import dateutil

class CreatedAt:
    
    @property
    def created_at(self):
        
        # get scenario header
        if self._scenario_header is None:
            self._get_scenario_header()
        
        # get created at
        datetime = self._scenario_header['created_at']
        
        # extract datetime in datetime format
        datetime = dateutil.parser.isoparse(datetime)
        
        # update timezone to system timezone
        datetime = datetime.astimezone(dateutil.tz.tzlocal())
        
        # make desired datetime output format
        datetime = datetime.strftime("%d/%m/%Y %H:%M:%S %Z")
        
        return datetime